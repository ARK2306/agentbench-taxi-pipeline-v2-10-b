"""Command-line interface."""

import argparse
from pathlib import Path

import duckdb

from taxi_pipeline.ingest import ingest, validate_month
from taxi_pipeline.report import report
from taxi_pipeline.transform import QualityError, transform


def main() -> None:
    parser = argparse.ArgumentParser(prog="taxi-pipeline")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ingest").add_argument("month")
    commands.add_parser("transform")
    commands.add_parser("report")
    commands.add_parser("run-all").add_argument("months", nargs="+")
    args = parser.parse_args()
    try:
        if args.command in {"ingest", "run-all"}:
            months = [args.month] if args.command == "ingest" else args.months
            for month in months:
                validate_month(month)
            for month in months:
                count = ingest(args.root, month)
                print(f"{month}: {count:,} raw rows (idempotent)", flush=True)
        if args.command in {"transform", "run-all"}:
            print(f"Transform passed; DQ run {transform(args.root)}", flush=True)
        if args.command in {"report", "run-all"}:
            print(f"Report: {report(args.root)}", flush=True)
    except (QualityError, ValueError, OSError, duckdb.Error) as exc:
        parser.exit(1, f"taxi-pipeline: {exc}\n")


if __name__ == "__main__":
    main()
