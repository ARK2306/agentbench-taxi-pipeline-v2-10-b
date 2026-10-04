"""Command-line interface."""

import argparse
from pathlib import Path

from taxi_pipeline.ingest import ingest


def main() -> None:
    parser = argparse.ArgumentParser(prog="taxi-pipeline")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ingest").add_argument("month")
    args = parser.parse_args()
    count = ingest(args.root, args.month)
    print(f"{args.month}: {count:,} raw rows (idempotent)")


if __name__ == "__main__":
    main()
