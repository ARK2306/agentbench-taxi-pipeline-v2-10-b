"""Incremental cleaning, auditable quality gates, and aggregate models."""

import uuid
from pathlib import Path

from taxi_pipeline.ingest import ZONE_URL, connect, download

MODEL_VERSION = "1"
KEY_COLUMNS = (
    "source_month",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "trip_distance",
    "tip_amount",
)
CLEAN_SELECT = """
    SELECT DISTINCT *,
        epoch(tpep_dropoff_datetime - tpep_pickup_datetime) AS duration_seconds
    FROM raw_trips
    WHERE source_month = ?
        AND fare_amount > 0 AND isfinite(fare_amount)
        AND trip_distance > 0 AND isfinite(trip_distance)
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND tpep_pickup_datetime >= CAST(source_month || '-01' AS DATE)
        AND tpep_pickup_datetime < CAST(source_month || '-01' AS DATE) + INTERVAL 1 MONTH
"""


class QualityError(RuntimeError):
    """Quality gates failed; results remain available in dq_results."""


def check_quality(con) -> str:
    """Record every check, including failures, before raising to the caller."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, checked_at TIMESTAMP, source_month VARCHAR,
            check_name VARCHAR, passed BOOLEAN, observed DOUBLE,
            requirement VARCHAR, details VARCHAR
        )
    """)
    run_id = str(uuid.uuid4())
    counts = con.execute("""
        SELECT r.source_month, r.n, coalesce(t.n, 0)
        FROM (SELECT source_month, count(*) n FROM raw_trips GROUP BY 1) r
        LEFT JOIN (SELECT source_month, count(*) n FROM trips GROUP BY 1) t USING(source_month)
        ORDER BY 1
    """).fetchall()
    results = []
    previous = None
    for month, raw, clean in counts:
        survival = clean / raw
        results.append((month, "survival", survival >= 0.95, survival, ">= 0.95", f"{clean}/{raw}"))
        nulls = con.execute(
            "SELECT count(*) FROM trips WHERE source_month=? AND ("
            + " OR ".join(f"{column} IS NULL" for column in KEY_COLUMNS)
            + ")",
            [month],
        ).fetchone()[0]
        results.append((month, "key_nulls", nulls == 0, nulls, "= 0", ", ".join(KEY_COLUMNS)))
        unmatched = con.execute(
            """
            SELECT count(*) FROM trips t LEFT JOIN zones z ON t.PULocationID=z.LocationID
            WHERE t.source_month=? AND z.LocationID IS NULL
        """,
            [month],
        ).fetchone()[0]
        results.append(
            (month, "pickup_zones", unmatched == 0, unmatched, "= 0", "Unmatched pickups")
        )
        ordinal = int(month[:4]) * 12 + int(month[5:])
        if previous and ordinal - previous[0] == 1:
            change = abs(clean - previous[1]) / previous[1] if previous[1] else float("inf")
            results.append(
                (
                    month,
                    "month_over_month",
                    change < 0.40,
                    change,
                    "< 0.40",
                    f"Absolute relative change from {previous[2]}",
                )
            )
        else:
            results.append(
                (
                    month,
                    "month_over_month",
                    True,
                    None,
                    "< 0.40",
                    "Not applicable: preceding calendar month not ingested",
                )
            )
        previous = (ordinal, clean, month)
    # Empty monthly inputs are not valid datasets.
    for (month,) in con.execute("""
        SELECT source_month FROM ingestion_log EXCEPT SELECT source_month FROM raw_trips
    """).fetchall():
        results.append((month, "survival", False, 0, ">= 0.95", "Empty raw month"))
    if not results:
        raise ValueError("No ingested months; run ingest first")
    con.executemany(
        """
        INSERT INTO dq_results VALUES (?, current_timestamp, ?, ?, ?, ?, ?, ?)
    """,
        [(run_id, *row) for row in results],
    )
    failed = [f"{row[0]}:{row[1]} ({row[3]})" for row in results if not row[2]]
    if failed:
        raise QualityError("DQ failed: " + "; ".join(failed))
    return run_id


def aggregate(con) -> None:
    con.execute("""
        CREATE OR REPLACE TABLE daily_trips AS
        SELECT source_month, CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
            count(*) AS trip_count, sum(fare_amount) AS total_fare,
            sum(tip_amount) AS total_tip, avg(trip_distance) AS avg_distance
        FROM trips GROUP BY 1, 2 ORDER BY 1, 2
    """)
    con.execute("""
        CREATE OR REPLACE TABLE hourly_fare_tip AS
        SELECT source_month, hour(tpep_pickup_datetime) AS pickup_hour,
            count(*) AS trip_count, sum(fare_amount) AS total_fare,
            sum(tip_amount) AS total_tip, avg(fare_amount) AS avg_fare,
            100.0 * sum(tip_amount) / sum(fare_amount) AS tip_pct
        FROM trips GROUP BY 1, 2 ORDER BY 1, 2
    """)
    con.execute("""
        CREATE OR REPLACE TABLE top_zones AS
        SELECT t.source_month, t.PULocationID AS location_id, z.Borough AS borough,
            z.Zone AS zone, count(*) AS trip_count, sum(t.fare_amount) AS total_fare
        FROM trips t JOIN zones z ON t.PULocationID=z.LocationID
        GROUP BY 1, 2, 3, 4 ORDER BY 1, trip_count DESC, location_id
    """)


def transform(root: Path) -> str:
    with connect(root) as con:
        if not con.execute("SELECT count(*) FROM ingestion_log").fetchone()[0]:
            raise ValueError("No ingested months; run ingest first")
        zone_path = download(ZONE_URL, root / "data/raw/taxi_zone_lookup.csv")
        con.execute(
            """
            CREATE OR REPLACE TABLE zones AS
            SELECT * FROM read_csv(?, header=true, columns={
                'LocationID': 'INTEGER', 'Borough': 'VARCHAR',
                'Zone': 'VARCHAR', 'service_zone': 'VARCHAR'})
        """,
            [str(zone_path)],
        )
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS zones_id ON zones(LocationID)")
        con.execute("""
            CREATE TABLE IF NOT EXISTS transformed_months (
                source_month VARCHAR PRIMARY KEY, sha256 VARCHAR, model_version VARCHAR)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_state (run_id VARCHAR, ready BOOLEAN)
        """)
        con.execute("DELETE FROM pipeline_state")
        con.execute("INSERT INTO pipeline_state VALUES (NULL, false)")
        con.execute("CREATE TABLE IF NOT EXISTS trips AS " + CLEAN_SELECT + " LIMIT 0", ["0001-01"])
        pending = con.execute(
            """
            SELECT i.source_month, i.sha256 FROM ingestion_log i
            LEFT JOIN transformed_months t USING(source_month)
            WHERE t.source_month IS NULL OR t.sha256 != i.sha256 OR t.model_version != ?
            ORDER BY 1
        """,
            [MODEL_VERSION],
        ).fetchall()
        con.execute("BEGIN")
        try:
            for month, digest in pending:
                con.execute("DELETE FROM trips WHERE source_month=?", [month])
                con.execute("INSERT INTO trips BY NAME " + CLEAN_SELECT, [month])
                con.execute(
                    "INSERT OR REPLACE INTO transformed_months VALUES (?, ?, ?)",
                    [month, digest, MODEL_VERSION],
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        run_id = check_quality(con)
        con.execute("BEGIN")
        try:
            aggregate(con)
            con.execute("UPDATE pipeline_state SET run_id=?, ready=true", [run_id])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return run_id
