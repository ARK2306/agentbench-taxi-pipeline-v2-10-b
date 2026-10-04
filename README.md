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

## Run the complete pipeline

Use Python 3.14 (used locally and by CI); Python >=3.12 is supported by the package.
Install the complete pinned environment for reproducibility:

```sh
python3 -m venv .venv
PIP_CACHE_DIR=./.cache/pip .venv/bin/python -m pip install -r requirements.lock
PIP_CACHE_DIR=./.cache/pip .venv/bin/python -m pip install --no-build-isolation --no-deps -e .
.venv/bin/taxi-pipeline run-all 2024-01 2024-02 2024-03
# Equivalent incremental stages:
.venv/bin/taxi-pipeline ingest 2024-01
.venv/bin/taxi-pipeline transform
.venv/bin/taxi-pipeline report
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pytest -q
```

`--root PATH` before the command changes the output workspace (default: current
directory). `transform` and `report` cover **all ingested months**. `run-all` accepts
one or more explicit YYYY-MM months. TLC's official January–March 2024 sources:

- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
- https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

## Models and quality

`trips` preserves source fields, source month, and duration in seconds. Cleaning
removes exact duplicate rows, non-positive/non-finite fares or distances,
non-positive durations, and pickups outside their source file's month. A
`transformed_months` ledger tracks source hashes and model version, so unchanged
months are not cleaned again. Increment `MODEL_VERSION` when changing cleaning
logic. Aggregates are rebuilt across the resulting snapshot on successful DQ:

| Table | Grain | Measures |
|---|---|---|
| daily_trips | month, pickup date | count, total fare/tip, average distance |
| hourly_fare_tip | month, pickup hour | count, total/average fare, total tip, tip % |
| top_zones | month, pickup location | count, total fare, official zone and borough |

`top_zones` keeps all pickup zones; the chart selects the top 10 across all months.
Tip percentage is `100 * sum(tip_amount) / sum(fare_amount)` across all payment
types. Cash tips are not captured by TLC. Times use TLC's local NYC wall-clock
values without timezone conversion.

Each transform records four checks per month in append-only `dq_results` with
run ID, timestamp, observed value, requirement, status, and details:

- Survival is **at least 95%** of original raw rows (duplicates remain in denominator).
- No nulls in source month, pickup/dropoff timestamps, pickup/dropoff IDs, fare,
  distance, or tip. Passenger count and vendor metadata are allowed to be null.
- Every cleaned pickup location matches the official zone lookup.
- Absolute cleaned-count change from the previous calendar month is **less than
  40%**. A first month or a gap is recorded as N/A rather than compared across gaps.

Failed DQ persists and exits nonzero; aggregates are published only on success.
Report generation refuses failed or stale transforms. Ingestion is transactional;
partial downloads never replace complete files. Only one writer should run at a
time (DuckDB enforces the database write lock). To rebuild from cached inputs,
remove the local warehouse and rerun `run-all`. Cached files are intentionally
immutable; source SHA-256 values in the report identify the exact inputs.

## Outputs and automation

`reports/report.md` references three sibling PNGs and contains monthly totals,
DQ results, methodology, and source hashes. Keep these files together when viewing.
Three aggregate Parquet exports are written alongside the report.

The `agentbench-ci` workflow runs ruff, formatting checks, and synthetic Parquet
unit tests on push and PR. Manual dispatch and a Monday weekly schedule also run
the `pipeline` job for January 2024, cache raw Parquet by month, and upload
`agentbench-taxi-reports-2024-01`. The `agentbench-release` workflow runs all three
months on `v*` tags and publishes seven release assets: Markdown, three PNGs, and
three Parquet tables. No dataset downloads are required for unit tests.
