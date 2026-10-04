"""Atomic downloads and transactional, month-idempotent ingestion."""

import hashlib
import re
import shutil
import urllib.request
from datetime import date
from pathlib import Path

import duckdb

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_URL = f"{BASE_URL}/misc/taxi_zone_lookup.csv"


def validate_month(month: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise ValueError("Month must be YYYY-MM")
    date.fromisoformat(f"{month}-01")
    return month


def connect(root: Path) -> duckdb.DuckDBPyConnection:
    (root / "data").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(root / "data/warehouse.duckdb"))
    con.execute("SET threads=4")
    con.execute("SET memory_limit='2GB'")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            source_month VARCHAR PRIMARY KEY, row_count BIGINT NOT NULL,
            sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    return con


def download(url: str, target: Path) -> Path:
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".part")
    try:
        with urllib.request.urlopen(url, timeout=120) as response, partial.open("wb") as out:
            shutil.copyfileobj(response, out)
        partial.replace(target)
    finally:
        partial.unlink(missing_ok=True)
    return target


def ingest(root: Path, month: str) -> int:
    validate_month(month)
    url = f"{BASE_URL}/trip-data/yellow_tripdata_{month}.parquet"
    path = download(url, root / "data/raw" / f"yellow_tripdata_{month}.parquet")
    with connect(root) as con:
        existing = con.execute(
            "SELECT row_count FROM ingestion_log WHERE source_month=?", [month]
        ).fetchone()
        if existing:
            return existing[0]
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        con.execute("BEGIN")
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_trips AS
                SELECT ?::VARCHAR AS source_month, * FROM read_parquet(?) LIMIT 0
            """,
                [month, str(path)],
            )
            con.execute(
                """
                INSERT INTO raw_trips BY NAME
                SELECT ?::VARCHAR AS source_month, * FROM read_parquet(?)
            """,
                [month, str(path)],
            )
            count = con.execute(
                "SELECT count(*) FROM raw_trips WHERE source_month=?", [month]
            ).fetchone()[0]
            con.execute("CREATE INDEX IF NOT EXISTS raw_month_idx ON raw_trips(source_month)")
            con.execute(
                """
                INSERT INTO ingestion_log (source_month, row_count, sha256, source_url)
                VALUES (?, ?, ?, ?)
            """,
                [month, count, digest, url],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return count
