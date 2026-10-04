# agentbench-taxi-pipeline

Reproducible NYC TLC yellow taxi analysis using Python and persistent DuckDB.

```sh
python3 -m venv .venv
PIP_CACHE_DIR=./.cache/pip .venv/bin/python -m pip install -e '.[dev]'
.venv/bin/taxi-pipeline ingest 2024-01
```

Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Downloads are atomic and cached in `data/raw/`. Each transaction loads one month
into `data/warehouse.duckdb` and records its SHA-256 and row count. Re-ingestion
skips committed months. `raw_trips` is a native DuckDB table with logical monthly
partitions (`source_month`), month-clustered inserts, and a month index. DuckDB
has no native `PARTITION BY` table DDL; the source files provide physical monthly
separation. Raw data and the local environment are excluded from Git.
