from datetime import datetime

import pytest

from taxi_pipeline.ingest import connect
from taxi_pipeline.report import report
from taxi_pipeline.transform import QualityError, transform


def latest_check(root, name, month="2024-01"):
    with connect(root) as con:
        return con.execute(
            """SELECT passed, observed FROM dq_results
            WHERE check_name=? AND source_month=? ORDER BY checked_at DESC LIMIT 1""",
            [name, month],
        ).fetchone()


def test_cleaning_all_rules_and_duplicates(dataset):
    root = dataset(
        count=200,
        edits={
            0: {"fare_amount": 0},
            1: {"fare_amount": -1},
            2: {"trip_distance": 0},
            3: {"trip_distance": -1},
            4: {"tpep_dropoff_datetime": datetime(2024, 1, 1)},
            5: {"tpep_pickup_datetime": datetime(2023, 12, 31)},
            6: {"tpep_pickup_datetime": datetime(2024, 2, 1)},
            7: {"tpep_dropoff_datetime": datetime(2024, 1, 1, 1, 10)},
            8: {"fare_amount": float("inf")},
        },
    )
    with connect(root) as con:
        con.execute(
            "INSERT INTO raw_trips SELECT * FROM raw_trips WHERE "
            "tpep_pickup_datetime='2024-01-01 01:30:00'"
        )
    transform(root)
    with connect(root) as con:
        assert con.execute("SELECT count(*) FROM trips").fetchone()[0] == 191
        assert con.execute("SELECT min(duration_seconds) FROM trips").fetchone()[0] > 0


def test_aggregates(dataset):
    root = dataset(count=12)
    transform(root)
    with connect(root) as con:
        assert con.execute(
            "SELECT trip_count, total_fare, total_tip FROM daily_trips"
        ).fetchone() == (12, 120, 24)
        assert con.execute(
            "SELECT pickup_hour, trip_count, avg_fare, tip_pct FROM hourly_fare_tip ORDER BY 1"
        ).fetchall() == [(0, 6, 10, 20), (1, 6, 10, 20)]
        assert con.execute("SELECT zone, trip_count FROM top_zones ORDER BY zone").fetchall() == [
            ("Alpha", 6),
            ("Beta", 6),
        ]


@pytest.mark.parametrize("invalid,passed", [(5, True), (6, False)])
def test_survival_boundary(dataset, invalid, passed):
    root = dataset(edits={i: {"fare_amount": 0} for i in range(invalid)})
    if passed:
        transform(root)
    else:
        with pytest.raises(QualityError, match="survival"):
            transform(root)
    assert latest_check(root, "survival") == (passed, (100 - invalid) / 100)


@pytest.mark.parametrize("column", ["PULocationID", "DOLocationID", "tip_amount"])
def test_key_nulls_fail_and_persist(dataset, column):
    root = dataset(edits={0: {column: None}})
    with pytest.raises(QualityError, match="key_nulls"):
        transform(root)
    assert latest_check(root, "key_nulls") == (False, 1)
    with pytest.raises(ValueError, match="DQ failed"):
        report(root)


def test_unknown_pickup_zone_fails(dataset):
    root = dataset(edits={0: {"PULocationID": 999}})
    with pytest.raises(QualityError, match="pickup_zones"):
        transform(root)
    assert latest_check(root, "pickup_zones") == (False, 1)


@pytest.mark.parametrize("second,passed", [(139, True), (140, False), (61, True), (60, False)])
def test_month_over_month_strict_boundary(dataset, second, passed):
    root = dataset()
    dataset("2024-02", count=second)
    if passed:
        transform(root)
    else:
        with pytest.raises(QualityError, match="month_over_month"):
            transform(root)
    assert latest_check(root, "month_over_month", "2024-02")[0] is passed


def test_success_incremental_and_gap(dataset):
    root = dataset(duplicate=True)
    transform(root)
    with connect(root) as con:
        first = con.execute("SELECT * FROM trips ORDER BY tpep_pickup_datetime").fetchall()
    dataset("2024-03", count=150)
    transform(root)
    transform(root)
    with connect(root) as con:
        assert (
            con.execute(
                "SELECT * FROM trips WHERE source_month='2024-01' ORDER BY tpep_pickup_datetime"
            ).fetchall()
            == first
        )
        assert con.execute("SELECT count(*) FROM trips").fetchone()[0] == 250
        assert con.execute("SELECT bool_and(passed) FROM dq_results").fetchone()[0]
    assert latest_check(root, "month_over_month", "2024-03") == (True, None)


def test_report_outputs_and_stale_guard(dataset):
    root = dataset()
    transform(root)
    path = report(root)
    assert "100.00%" in path.read_text()
    assert "PASS" in path.read_text()
    for name in ["daily_trips", "hourly_fare_tip", "top_zones"]:
        assert (path.parent / f"{name}.png").read_bytes().startswith(b"\x89PNG")
        with connect(root) as con:
            assert (
                con.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(path.parent / f"{name}.parquet")]
                ).fetchone()[0]
                > 0
            )
    dataset("2024-02")
    with pytest.raises(ValueError, match="stale"):
        report(root)


def test_empty_warehouse(tmp_path):
    with pytest.raises(ValueError, match="No ingested"):
        transform(tmp_path)
