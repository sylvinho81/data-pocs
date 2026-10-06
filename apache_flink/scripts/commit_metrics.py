#!/usr/bin/env python3
"""Show Iceberg CommitReport metrics rolled up by producer ingestion time.

CommitReports are written by ``JsonFileMetricsReporter`` on each Iceberg snapshot
commit (one Flink checkpoint). This script joins those reports with the table
snapshots and, for ``earthquakes_raw``, counts rows by ``ingested_at``.

Run inside the compose network:

    ./scripts/commit_metrics.sh
    ./scripts/commit_metrics.sh --bucket hour --last 0
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import DataFileContent


DEFAULT_TABLES = (
    "earthquakes.earthquakes_raw",
    "earthquakes.earthquakes_by_minute",
)


def main() -> int:
    args = parse_args()
    reports = load_reports(args.metrics_path)
    print_header(args, reports)

    if args.reports_only:
        print_reports(reports, args)
        return 0

    try:
        catalog = connect_catalog()
    except Exception as exc:
        print(f"\nCould not open the Iceberg catalog ({exc}).")
        print("Showing CommitReports without ingestion-time counts.\n")
        print_reports(reports, args)
        return 1

    exit_code = 0
    for identifier in args.tables:
        try:
            table = catalog.load_table(identifier)
        except Exception as exc:
            print(f"\n=== {identifier} ===")
            print(f"not available: {exc}")
            exit_code = 1
            continue
        show_table(identifier, table, reports, args)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        choices=("minute", "hour"),
        default=os.getenv("COMMIT_METRICS_BUCKET", "minute"),
        help="ingestion-time bucket (default: minute)",
    )
    parser.add_argument(
        "--last",
        type=int,
        default=int(os.getenv("COMMIT_METRICS_LAST", "40")),
        help="number of most recent commits to show; 0 shows all",
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        help="catalog table to show (repeatable). Default: both earthquake tables",
    )
    parser.add_argument(
        "--metrics-path",
        default=os.getenv("COMMIT_METRICS_PATH", "metrics/commits.jsonl"),
        help="CommitReport JSONL written by the Flink reporter",
    )
    parser.add_argument(
        "--reports-only",
        action="store_true",
        help="print CommitReports and skip catalog / data-file reads",
    )
    args = parser.parse_args()
    if not args.tables:
        args.tables = list(DEFAULT_TABLES)
    return args


def connect_catalog():
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:18181"),
            "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:19000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": os.getenv("AWS_REGION", "us-east-1"),
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


def load_reports(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    reports = []
    skipped = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                reports.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
    if skipped:
        print(f"Skipped {skipped} malformed line(s) in {path}", file=sys.stderr)
    # A retried commit can emit the same snapshot twice. Keep the latest report.
    deduped: dict[tuple[str, int], dict] = {}
    for report in reports:
        try:
            key = (short_table_name(report.get("table_name", "")), int(report["snapshot_id"]))
        except (TypeError, ValueError, KeyError):
            continue
        deduped[key] = report
    return list(deduped.values())


def print_header(args: argparse.Namespace, reports: list[dict]) -> None:
    print("Iceberg CommitReport metrics")
    print(f"reports: {len(reports)}  file: {args.metrics_path}")
    print(
        "Commit time is when Flink published the snapshot. "
        "Ingestion time is earthquakes_raw.ingested_at (producer publish time, UTC)."
    )
    if not reports:
        print(
            "No CommitReports yet. They appear after the rebuilt Flink job completes a "
            "checkpoint (~30s). Snapshot summaries are shown in the meantime; duration "
            "and attempts stay blank until the reporter records a commit."
        )


def print_reports(reports: list[dict], args: argparse.Namespace) -> None:
    selected = []
    for report in reports:
        name = short_table_name(report.get("table_name", ""))
        if any(name == short_table_name(table) or name.endswith(table) for table in args.tables):
            selected.append(report)
    selected.sort(key=lambda report: report.get("reported_at") or "")
    if args.last:
        selected = selected[-args.last :]
    if not selected:
        print("\nNo matching CommitReports.")
        return
    rows = []
    buckets: dict[str, dict] = defaultdict(lambda: {"records": 0, "commits": set()})
    for report in selected:
        records = report.get("added_records")
        when = bucket_label(parse_time(report.get("reported_at")), args.bucket)
        rows.append(
            [
                when,
                short_table_name(report.get("table_name", "")),
                report.get("sequence_number", "-"),
                report.get("operation") or "-",
                fmt_duration(report.get("duration_ms")),
                fmt_int(report.get("attempts")),
                fmt_int(report.get("added_data_files")),
                fmt_int(records),
                fmt_bytes(report.get("added_files_size_bytes")),
            ]
        )
        if when != "-" and records:
            buckets[when]["records"] += int(records)
            buckets[when]["commits"].add((short_table_name(report.get("table_name", "")), report.get("snapshot_id")))
    print("\nCommits (grouped below by report time, because data files were not read)\n")
    print(
        render(
            ["commit time", "table", "seq", "op", "duration", "tries", "+files", "+records", "+bytes"],
            rows,
        )
    )
    print("\nBy commit time\n")
    print(render_buckets(buckets))


def show_table(identifier: str, table, reports: list[dict], args: argparse.Namespace) -> None:
    name = short_table_name(identifier)
    by_snapshot = {
        int(report["snapshot_id"]): report
        for report in reports
        if short_table_name(report.get("table_name", "")) == name
        and report.get("snapshot_id") is not None
    }
    snapshots = {snapshot.snapshot_id: snapshot for snapshot in table.snapshots()}
    snapshot_ids = set(snapshots) | set(by_snapshot)
    ordered = sorted(snapshot_ids, key=lambda snapshot_id: commit_sort_key(snapshots.get(snapshot_id), by_snapshot.get(snapshot_id)))
    if args.last:
        ordered = ordered[-args.last :]

    print(f"\n=== {name}  snapshots={len(snapshots)}  reports={len(by_snapshot)} ===")
    if not ordered:
        print("No commits yet. Wait for a Flink checkpoint.")
        return

    has_ingested_at = "ingested_at" in {field.name for field in table.schema().fields}
    file_cache: dict[int | None, set[str]] = {}
    rows = []
    buckets: dict[str, dict] = defaultdict(lambda: {"records": 0, "commits": set()})
    ingestion_note = None

    for snapshot_id in ordered:
        snapshot = snapshots.get(snapshot_id)
        report = by_snapshot.get(snapshot_id)
        added_records = metric(report, "added_records", summary_int(snapshot, "added-records"))
        added_files = metric(report, "added_data_files", summary_int(snapshot, "added-data-files"))
        added_bytes = metric(
            report,
            "added_files_size_bytes",
            summary_int(snapshot, "added-files-size"),
        )
        counts = None
        if has_ingested_at:
            try:
                counts = ingestion_counts(table, snapshot, file_cache, args.bucket)
            except Exception as exc:
                has_ingested_at = False
                ingestion_note = f"Could not read ingested_at from data files ({exc})."
                counts = None

        if counts is not None:
            labels = sorted(label for label in counts if label != "unknown")
            if not labels:
                span = "-"
            elif len(labels) == 1:
                span = labels[0]
            else:
                span = f"{labels[0]} -> {labels[-1]}"
            for label, count in counts.items():
                if label == "-":
                    continue
                buckets[label]["records"] += count
                buckets[label]["commits"].add(snapshot_id)
        else:
            span = "-"
            label = bucket_label(commit_time(snapshot, report), args.bucket)
            if label != "-" and added_records:
                buckets[label]["records"] += int(added_records)
                buckets[label]["commits"].add(snapshot_id)

        rows.append(
            [
                fmt_time(commit_time(snapshot, report)),
                sequence_of(snapshot, report),
                operation_of(snapshot, report),
                fmt_duration(None if report is None else report.get("duration_ms")),
                fmt_int(None if report is None else report.get("attempts")),
                fmt_int(added_files),
                fmt_int(added_records),
                fmt_bytes(added_bytes),
                span,
            ]
        )

    if ingestion_note:
        print(ingestion_note)
        print("The rollup uses commit time instead.\n")
    elif has_ingested_at:
        print("Ingestion span counts rows in the data files each commit added.\n")
    else:
        print("No ingested_at column on this table. The rollup uses commit time.\n")

    print(
        render(
            ["commit time", "seq", "op", "duration", "tries", "+files", "+records", "+bytes", "ingestion"],
            rows,
        )
    )
    title = "By ingestion time" if has_ingested_at else "By commit time"
    print(f"\n{title} ({args.bucket})\n")
    print(render_buckets(buckets))


def ingestion_counts(table, snapshot, file_cache: dict, bucket: str) -> dict[str, int] | None:
    if snapshot is None:
        return None
    added = added_data_paths(table, snapshot, file_cache)
    if not added:
        return {}
    counts: dict[str, int] = defaultdict(int)
    for path in sorted(added):
        for value in read_ingested_at(table, path):
            label = bucket_label(coerce_datetime(value), bucket)
            counts["unknown" if label == "-" else label] += 1
    return dict(counts)


def added_data_paths(table, snapshot, file_cache: dict) -> set[str]:
    current = data_file_paths(table, snapshot.snapshot_id, file_cache)
    parent_id = snapshot.parent_snapshot_id
    if parent_id is None:
        return current
    parent = data_file_paths(table, parent_id, file_cache)
    return current - parent


def data_file_paths(table, snapshot_id: int | None, file_cache: dict) -> set[str]:
    if snapshot_id in file_cache:
        return file_cache[snapshot_id]
    if snapshot_id is None:
        file_cache[snapshot_id] = set()
        return file_cache[snapshot_id]
    paths = set()
    for task in table.scan(snapshot_id=snapshot_id).plan_files():
        data_file = task.file
        if data_file.content != DataFileContent.DATA:
            continue
        paths.add(data_file.file_path)
    file_cache[snapshot_id] = paths
    return paths


def read_ingested_at(table, path: str) -> list:
    import pyarrow.parquet as pq

    handle = table.io.new_input(path).open()
    try:
        payload = handle.read()
    finally:
        handle.close()
    column = pq.read_table(BytesIO(payload), columns=["ingested_at"]).column("ingested_at")
    return column.to_pylist()


def metric(report: dict | None, field: str, fallback):
    if report is not None and report.get(field) is not None:
        return report[field]
    return fallback


def summary_int(snapshot, key: str):
    if snapshot is None or snapshot.summary is None:
        return None
    try:
        raw = snapshot.summary[key]
    except Exception:
        return None
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def operation_of(snapshot, report) -> str:
    if report and report.get("operation"):
        return str(report["operation"])
    if snapshot is not None and snapshot.summary is not None:
        operation = snapshot.summary.operation
        return getattr(operation, "value", str(operation))
    return "-"


def sequence_of(snapshot, report) -> str:
    if snapshot is not None and snapshot.sequence_number is not None:
        return str(snapshot.sequence_number)
    if report and report.get("sequence_number") is not None:
        return str(report["sequence_number"])
    return "-"


def commit_time(snapshot, report) -> datetime | None:
    if snapshot is not None and snapshot.timestamp_ms:
        return datetime.fromtimestamp(snapshot.timestamp_ms / 1000, tz=timezone.utc)
    if report:
        return parse_time(report.get("reported_at"))
    return None


def commit_sort_key(snapshot, report) -> tuple:
    when = commit_time(snapshot, report)
    stamp = when.timestamp() if when else 0
    sequence = 0
    if snapshot is not None and snapshot.sequence_number is not None:
        sequence = snapshot.sequence_number
    elif report and report.get("sequence_number") is not None:
        sequence = int(report["sequence_number"])
    return (stamp, sequence)


def short_table_name(name: str) -> str:
    parts = [part for part in str(name).split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return ".".join(parts)


def parse_time(value) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def coerce_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return parse_time(value)


def bucket_label(value: datetime | None, bucket: str) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    if bucket == "hour":
        value = value.replace(minute=0, second=0, microsecond=0)
        return value.strftime("%Y-%m-%d %H:00Z")
    value = value.replace(second=0, microsecond=0)
    return value.strftime("%Y-%m-%d %H:%MZ")


def fmt_time(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def fmt_int(value) -> str:
    if value is None:
        return "-"
    return str(int(value))


def fmt_duration(value) -> str:
    if value is None:
        return "-"
    millis = float(value)
    if millis < 1000:
        return f"{millis:.0f} ms"
    return f"{millis / 1000:.2f} s"


def fmt_bytes(value) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GiB"


def render_buckets(buckets: dict[str, dict]) -> str:
    if not buckets:
        return "(no rows)"
    rows = []
    for label in sorted(buckets):
        rows.append([label, str(buckets[label]["records"]), str(len(buckets[label]["commits"]))])
    return render(["bucket", "records", "commits"], rows)


def render(headers: list[str], rows: list[list]) -> str:
    text_rows = [["" if cell is None else str(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in text_rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def format_row(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))

    lines = [format_row(headers), format_row(["-" * width for width in widths])]
    lines.extend(format_row(row) for row in text_rows)
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
