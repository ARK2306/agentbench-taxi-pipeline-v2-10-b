"""Deterministic report and portable aggregate exports."""

import os
from pathlib import Path

from taxi_pipeline.ingest import connect
from taxi_pipeline.transform import MODEL_VERSION


def report(root: Path) -> Path:
    os.environ.setdefault("MPLCONFIGDIR", str(root / ".cache/matplotlib"))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    with connect(root) as con:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "pipeline_state" not in tables:
            raise ValueError("Run transform successfully before report")
        state = con.execute("SELECT run_id, ready FROM pipeline_state").fetchone()
        pending = con.execute(
            """
            SELECT count(*) FROM ingestion_log i LEFT JOIN transformed_months t USING(source_month)
            WHERE t.source_month IS NULL OR i.sha256 != t.sha256 OR t.model_version != ?
        """,
            [MODEL_VERSION],
        ).fetchone()[0]
        if not state or not state[1] or pending:
            raise ValueError("Report refused: transforms are stale or DQ failed; run transform")
        output = root / "reports"
        output.mkdir(parents=True, exist_ok=True)
        plt.rcParams.update(
            {
                "figure.dpi": 150,
                "font.size": 10,
                "axes.spines.top": False,
                "axes.spines.right": False,
                "axes.titleweight": "bold",
            }
        )
        rows = con.execute("SELECT pickup_date, trip_count FROM daily_trips ORDER BY 1").fetchall()
        fig, ax = plt.subplots(figsize=(11, 4.5), layout="constrained")
        ax.plot([r[0] for r in rows], [r[1] for r in rows], color="#087e8b", linewidth=1.6)
        ax.set(
            title="NYC yellow taxi · trips per day", ylabel="Cleaned trips", xlabel="Pickup date"
        )
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        ax.grid(axis="y", alpha=0.2)
        fig.savefig(output / "daily_trips.png")
        plt.close(fig)

        rows = con.execute("""
            SELECT pickup_hour, sum(total_fare)/sum(trip_count),
                100*sum(total_tip)/sum(total_fare)
            FROM hourly_fare_tip GROUP BY 1 ORDER BY 1
        """).fetchall()
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), layout="constrained")
        for ax, index, title, label, color in [
            (axes[0], 1, "Average fare by pickup hour", "Fare (USD)", "#087e8b"),
            (axes[1], 2, "Tip percentage by pickup hour", "Tips / fares (%)", "#b66a13"),
        ]:
            ax.plot([r[0] for r in rows], [r[index] for r in rows], color=color, marker=".")
            ax.set(title=title, ylabel=label, xlabel="Local pickup hour", xticks=range(0, 24, 3))
            ax.grid(axis="y", alpha=0.2)
        fig.savefig(output / "hourly_fare_tip.png")
        plt.close(fig)

        rows = con.execute("""
            SELECT zone, sum(trip_count) n FROM top_zones
            GROUP BY location_id, zone ORDER BY n DESC, location_id LIMIT 10
        """).fetchall()[::-1]
        fig, ax = plt.subplots(figsize=(11, 5), layout="constrained")
        ax.barh([r[0] for r in rows], [r[1] for r in rows], color="#087e8b")
        ax.set(title="Top 10 pickup zones · all ingested months", xlabel="Cleaned trips")
        ax.ticklabel_format(axis="x", style="plain")
        fig.savefig(output / "top_zones.png")
        plt.close(fig)

        months = con.execute("""
            SELECT i.source_month, i.row_count, sum(d.trip_count), sum(d.total_fare),
                sum(d.total_tip), sum(d.total_fare)/sum(d.trip_count),
                100*sum(d.total_tip)/sum(d.total_fare)
            FROM ingestion_log i JOIN daily_trips d USING(source_month)
            GROUP BY 1, 2 ORDER BY 1
        """).fetchall()
        lines = [
            "# NYC yellow taxi report",
            "",
            "## Monthly comparison",
            "",
            "| Month | Raw trips | Clean trips | Retained | Fares (USD) | Tips (USD) |"
            " Avg fare | Tip % |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for month, raw, clean, fare, tip, avg, pct in months:
            lines.append(
                f"| {month} | {raw:,} | {clean:,} | {clean / raw:.2%} | {fare:,.2f} | "
                f"{tip:,.2f} | {avg:.2f} | {pct:.2f}% |"
            )
        lines += [
            "",
            "## Charts",
            "",
            "![Trips per day](daily_trips.png)",
            "",
            "![Hourly fare and tip percentage](hourly_fare_tip.png)",
            "",
            "![Top pickup zones](top_zones.png)",
            "",
            "## Data quality",
            "",
            "| Month | Check | Status | Observed | Requirement | Details |",
            "|---|---|---|---:|---|---|",
        ]
        for month, name, passed, value, requirement, details in con.execute(
            """
            SELECT source_month, check_name, passed, observed, requirement, details
            FROM dq_results WHERE run_id=? ORDER BY source_month, check_name
        """,
            [state[0]],
        ).fetchall():
            observed = "N/A" if value is None else f"{value:.6f}"
            lines.append(
                f"| {month} | {name} | {'PASS' if passed else 'FAIL'} | {observed} | "
                f"{requirement} | {details} |"
            )
        lines += [
            "",
            "## Method and provenance",
            "",
            "Source: [NYC TLC trip records](https://www.nyc.gov/site/tlc/about/"
            "tlc-trip-record-data.page) and its official taxi zone lookup. "
            "Pickup times are local NYC wall-clock times as supplied by TLC.",
            "",
            "Cleaning removes non-positive or non-finite fare/distance, non-positive "
            "duration, out-of-source-month pickups, and exact duplicate source rows. "
            "DQ keys: source month, pickup/dropoff times and location IDs, fare, "
            "distance, and tip. Nullable passenger counts and vendor metadata are not keys.",
            "",
            "Tip percentage is 100 × sum(tips) / sum(fares), across all payment types. "
            "Cash tips are not recorded by TLC; this is recorded tip share. "
            "Month-over-month comparisons use cleaned counts and adjacent calendar months; "
            "the first month or a missing preceding month is explicitly N/A.",
            "",
            "## Source checksums",
            "",
            "| Month | SHA-256 |",
            "|---|---|",
        ]
        for month, digest in con.execute(
            "SELECT source_month, sha256 FROM ingestion_log ORDER BY 1"
        ).fetchall():
            lines.append(f"| {month} | `{digest}` |")
        for table, order in [
            ("daily_trips", "source_month, pickup_date"),
            ("hourly_fare_tip", "source_month, pickup_hour"),
            ("top_zones", "source_month, trip_count DESC, location_id"),
        ]:
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order}) TO ? "
                "(FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(output / f"{table}.parquet")],
            )
        path = output / "report.md"
        path.write_text("\n".join(lines) + "\n")
        return path
