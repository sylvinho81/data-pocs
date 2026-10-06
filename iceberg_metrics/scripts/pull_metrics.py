#!/usr/bin/env python3
"""Read Iceberg commit and scan metrics and store them in a second table.

Commit counters are already in each snapshot summary of the source table.
Scan counters are collected by planning a scan. Both are written to
``observability.reports`` as one row per report.

    python scripts/pull_metrics.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from iceberg_metrics.catalog import load_hive_catalog, recreate_table
from iceberg_metrics.reports import commit_rows, rows_to_arrow, scan_rows, summary_properties
from iceberg_metrics.schemas import REPORTS_SCHEMA


def main() -> int:
    source_namespace = os.getenv("SEED_NAMESPACE", "events")
    source_table_name = os.getenv("SEED_TABLE", "seeds")
    source_id = f"{source_namespace}.{source_table_name}"
    metrics_namespace = os.getenv("METRICS_NAMESPACE", "observability")
    metrics_table_name = os.getenv("METRICS_TABLE", "reports")
    metrics_id = f"{metrics_namespace}.{metrics_table_name}"
    row_filter = os.getenv("SCAN_FILTER", "city == 'Madrid'")

    catalog = load_hive_catalog()
    try:
        source = catalog.load_table(source_id)
    except Exception as exc:  # noqa: BLE001 - surface a missing seed table clearly
        print(f"could not load {source_id}: {exc}")
        print("run scripts/generate_seeds.py first")
        return 1

    explain(source, source_id, metrics_id)
    rows = commit_rows(source, source_id)
    rows.extend(scan_rows(source, source_id, row_filter))
    if not rows:
        print(f"{source_id} has no snapshots yet")
        return 1

    metrics = recreate_table(catalog, metrics_id, REPORTS_SCHEMA)
    metrics.append(rows_to_arrow(rows))
    metrics = catalog.load_table(metrics_id)

    print_commits(rows)
    print()
    print_scans(rows)
    print()
    print(f"wrote {len(rows)} report rows to {metrics_id}")
    print(f"metrics table location: {metrics.location()}")
    print(f"metrics metadata file:  {metrics.metadata_location}")
    sink_summary = summary_properties(metrics.current_snapshot())
    print(
        "that write is itself a commit: "
        f"added-data-files={sink_summary.get('added-data-files')} "
        f"added-records={sink_summary.get('added-records')} "
        f"added-files-size={sink_summary.get('added-files-size')}"
    )
    return 0


def explain(source, source_id: str, metrics_id: str) -> None:
    print(f"Source table {source_id}")
    print(f"  data files:     {source.location()}")
    print(f"  metadata file:  {source.metadata_location}")
    print()
    print("Where the numbers come from")
    print("  Hive stores the pointer to the metadata file. The counters themselves")
    print("  are in each snapshot summary inside that JSON. PyIceberg writes the")
    print("  file to MinIO on every commit, then reads the summaries back.")
    print("  Duration is snapshot timestamp minus commit-started-at-epoch-ms, which")
    print("  the seed writer adds to the same summary. Attempts are commit-attempts.")
    print("  Scan counters (manifests scanned and skipped, planning time) are")
    print("  produced while planning a query. They are recorded below, then stored")
    print(f"  as rows in {metrics_id}.")
    print()


def print_commits(rows: list[dict]) -> None:
    commits = [row for row in rows if row["report_type"] == "commit"]
    print(f"Commit reports ({len(commits)})")
    headers = (
        "snapshot_id",
        "operation",
        "files+",
        "records+",
        "bytes+",
        "total_files",
        "duration_ms",
        "attempts",
        "events",
    )
    body = []
    for row in commits:
        attributes = json.loads(row["attributes_json"])
        body.append(
            (
                row["snapshot_id"],
                row["operation"],
                row["added_data_files"],
                row["added_records"],
                row["added_files_size_bytes"],
                row["total_data_files"],
                _millis(row["duration_ns"]),
                row["attempts"],
                attributes.get("seed.event_ids"),
            )
        )
    _print_table(headers, body)


def print_scans(rows: list[dict]) -> None:
    scans = [row for row in rows if row["report_type"] == "scan"]
    print(f"Scan reports ({len(scans)})")
    headers = (
        "filter",
        "planning_ms",
        "result_files",
        "manifests",
        "scanned",
        "skipped",
        "skipped_files",
        "result_bytes",
    )
    body = []
    for row in scans:
        body.append(
            (
                row["filter"] or "(all rows)",
                _millis(row["duration_ns"]),
                row["result_data_files"],
                row["total_data_manifests"],
                row["scanned_data_manifests"],
                row["skipped_data_manifests"],
                row["skipped_data_files"],
                row["result_file_size_bytes"],
            )
        )
    _print_table(headers, body)


def _millis(duration_ns) -> str:
    if duration_ns is None:
        return ""
    return f"{duration_ns / 1_000_000:.1f}"


def _print_table(headers: tuple[str, ...], body: list[tuple]) -> None:
    rendered = [["" if value is None else str(value) for value in row] for row in body]
    widths = [len(header) for header in headers]
    for cells in rendered:
        for index, cell in enumerate(cells):
            widths[index] = max(widths[index], len(cell))

    def format_row(cells: list[str]) -> str:
        return "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(cells))

    print(format_row(list(headers)))
    print(format_row(["-" * width for width in widths]))
    for cells in rendered:
        print(format_row(cells))


if __name__ == "__main__":
    raise SystemExit(main())
