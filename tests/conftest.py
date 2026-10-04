from datetime import datetime, timedelta

import duckdb
import pytest

from taxi_pipeline.ingest import ingest


@pytest.fixture
def dataset(tmp_path):
    """Create tiny source Parquet files with no network or external fixtures."""
    raw = tmp_path / "data/raw"
    raw.mkdir(parents=True)
    (raw / "taxi_zone_lookup.csv").write_text(
        "LocationID,Borough,Zone,service_zone\n1,Manhattan,Alpha,Yellow Zone\n"
        "2,Queens,Beta,Yellow Zone\n"
    )

    def create(month="2024-01", count=100, edits=None, duplicate=False):
        start = datetime.fromisoformat(month + "-01T00:00:00")
        rows = []
        for i in range(count):
            pickup = start + timedelta(minutes=10 * i)
            row = dict(
                VendorID=1,
                tpep_pickup_datetime=pickup,
                tpep_dropoff_datetime=pickup + timedelta(minutes=5),
                PULocationID=1 if i % 2 == 0 else 2,
                DOLocationID=2,
                fare_amount=10.0,
                trip_distance=2.0,
                tip_amount=2.0,
            )
            row.update((edits or {}).get(i, {}))
            rows.append(list(row.values()))
        if duplicate:
            rows.append(rows[0])
        with duckdb.connect() as con:
            con.execute("""CREATE TABLE fixture (
                VendorID INTEGER, tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP, PULocationID INTEGER,
                DOLocationID INTEGER, fare_amount DOUBLE, trip_distance DOUBLE, tip_amount DOUBLE
            )""")
            con.executemany("INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            con.execute(
                "COPY fixture TO ? (FORMAT PARQUET)",
                [str(raw / f"yellow_tripdata_{month}.parquet")],
            )
        ingest(tmp_path, month)
        return tmp_path

    return create
