import duckdb
import pytest

from taxi_pipeline.ingest import connect, ingest, validate_month


def test_month_validation():
    assert validate_month("2024-03") == "2024-03"
    for month in ["2024-13", "2024-1", "2024-01'; DROP TABLE raw_trips;--"]:
        with pytest.raises(ValueError):
            validate_month(month)


def test_idempotent_ingestion(tmp_path):
    raw = tmp_path / "data/raw"
    raw.mkdir(parents=True)
    with duckdb.connect() as con:
        con.execute(
            "COPY (SELECT 1 AS id) TO ? (FORMAT PARQUET)",
            [str(raw / "yellow_tripdata_2024-01.parquet")],
        )
    assert ingest(tmp_path, "2024-01") == 1
    assert ingest(tmp_path, "2024-01") == 1
    with connect(tmp_path) as con:
        assert con.execute("SELECT count(*) FROM raw_trips").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM ingestion_log").fetchone()[0] == 1
