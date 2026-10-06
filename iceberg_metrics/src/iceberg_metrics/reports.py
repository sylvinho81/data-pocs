"""Turn Iceberg snapshot metadata and scan planning into report rows.

Java Iceberg emits the same numbers through ``MetricsReporter`` (``CommitReport``
and ``ScanReport``). Those callbacks stay inside the writer process. The counters
that survive a commit are the snapshot summary fields inside the table metadata
file. Scan counters are produced again here by planning a scan with PyIceberg.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.expressions.parser import parse
from pyiceberg.expressions.visitors import inclusive_projection, manifest_evaluator
from pyiceberg.manifest import ManifestContent

from iceberg_metrics.schemas import REPORTS_ARROW

# Snapshot summary key -> (column on observability.reports, OTEL-style metric name).
# "deleted-*" in the summary is the stored name of what CommitReport calls "removed".
COMMIT_COUNTERS: tuple[tuple[str, str, str], ...] = (
    ("added-data-files", "added_data_files", "iceberg.commit.data_files.added"),
    ("deleted-data-files", "removed_data_files", "iceberg.commit.data_files.removed"),
    ("total-data-files", "total_data_files", "iceberg.commit.data_files.total"),
    ("added-delete-files", "added_delete_files", "iceberg.commit.delete_files.added"),
    ("removed-delete-files", "removed_delete_files", "iceberg.commit.delete_files.removed"),
    ("total-delete-files", "total_delete_files", "iceberg.commit.delete_files.total"),
    ("added-equality-delete-files", "added_equality_delete_files", "iceberg.commit.equality_delete_files.added"),
    ("added-position-delete-files", "added_positional_delete_files", "iceberg.commit.positional_delete_files.added"),
    ("removed-equality-delete-files", "removed_equality_delete_files", "iceberg.commit.equality_delete_files.removed"),
    ("removed-position-delete-files", "removed_positional_delete_files", "iceberg.commit.positional_delete_files.removed"),
    ("added-records", "added_records", "iceberg.commit.records.added"),
    ("deleted-records", "removed_records", "iceberg.commit.records.removed"),
    ("total-records", "total_records", "iceberg.commit.records.total"),
    ("added-files-size", "added_files_size_bytes", "iceberg.commit.file_size_bytes.added"),
    ("removed-files-size", "removed_files_size_bytes", "iceberg.commit.file_size_bytes.removed"),
    ("total-files-size", "total_files_size_bytes", "iceberg.commit.file_size_bytes.total"),
    ("added-position-deletes", "added_positional_deletes", "iceberg.commit.positional_deletes.added"),
    ("removed-position-deletes", "removed_positional_deletes", "iceberg.commit.positional_deletes.removed"),
    ("total-position-deletes", "total_positional_deletes", "iceberg.commit.positional_deletes.total"),
    ("added-equality-deletes", "added_equality_deletes", "iceberg.commit.equality_deletes.added"),
    ("removed-equality-deletes", "removed_equality_deletes", "iceberg.commit.equality_deletes.removed"),
    ("total-equality-deletes", "total_equality_deletes", "iceberg.commit.equality_deletes.total"),
)

_COLUMN_NAMES = {field.name for field in REPORTS_ARROW}


def summary_properties(snapshot) -> dict[str, str]:
    """Return the snapshot summary as a string map, including ``operation``."""
    summary = snapshot.summary
    if summary is None:
        return {}
    extra = getattr(summary, "additional_properties", None)
    if isinstance(extra, dict):
        props = {str(key): str(value) for key, value in extra.items()}
    else:
        props = {}
        for key in list(summary):
            if key == "operation":
                continue
            value = summary[key]
            if value is not None:
                props[str(key)] = str(value)
    operation = getattr(summary, "operation", None)
    if operation is not None:
        props["operation"] = getattr(operation, "value", str(operation))
    return props


def commit_rows(table, table_name: str) -> list[dict]:
    """One commit row per snapshot. Counters come from metadata, not a log file."""
    rows = []
    for snapshot in table.snapshots():
        props = summary_properties(snapshot)
        metrics: dict[str, int] = {}
        row = _empty_row()
        row.update(
            {
                "report_id": str(uuid.uuid4()),
                "observed_at": datetime.fromtimestamp(snapshot.timestamp_ms / 1000, tz=timezone.utc),
                "report_type": "commit",
                "source": "snapshot-summary",
                "service_name": props.get("otel.service.name", "iceberg-seed-writer"),
                "table_name": table_name,
                "snapshot_id": snapshot.snapshot_id,
                "sequence_number": snapshot.sequence_number,
                "operation": props.get("operation"),
            }
        )
        for summary_key, column, metric_name in COMMIT_COUNTERS:
            if summary_key not in props:
                continue
            value = int(props[summary_key])
            metrics[metric_name] = value
            if column in _COLUMN_NAMES:
                row[column] = value

        started_ms = props.get("commit-started-at-epoch-ms")
        if started_ms is not None:
            duration_ns = max(0, int(snapshot.timestamp_ms) - int(started_ms)) * 1_000_000
            row["duration_ns"] = duration_ns
            metrics["iceberg.commit.duration_ns"] = duration_ns
        if "commit-attempts" in props:
            attempts = int(props["commit-attempts"])
            row["attempts"] = attempts
            metrics["iceberg.commit.attempts"] = attempts

        row["attributes_json"] = json.dumps(
            {
                "otel.scope.name": "iceberg.metrics",
                "otel.scope.schema_url": "https://iceberg.apache.org/docs/latest/metrics-reporting/",
                "iceberg.metadata.location": table.metadata_location,
                "iceberg.table.location": table.location(),
                "manifest_list": snapshot.manifest_list,
                "seed.batch_size": props.get("seed.batch-size"),
                "seed.event_ids": props.get("seed.event-ids") or props.get("seed.event-id"),
                "commit.started_at_epoch_ms": started_ms,
                "snapshot.summary": props,
            },
            sort_keys=True,
        )
        row["metrics_json"] = json.dumps(metrics, sort_keys=True)
        rows.append(row)
    rows.sort(key=lambda item: (item["observed_at"], item["snapshot_id"]))
    return rows


def scan_rows(table, table_name: str, row_filter: str | None) -> list[dict]:
    """Plan an unfiltered scan and one filtered scan, and record ScanReport counters."""
    rows = [
        _scan_row(table, table_name, None),
        _scan_row(table, table_name, row_filter),
    ]
    return rows


def rows_to_arrow(rows: list[dict]) -> pa.Table:
    columns = {
        field.name: pa.array([row.get(field.name) for row in rows], type=field.type)
        for field in REPORTS_ARROW
    }
    return pa.table(columns, schema=REPORTS_ARROW)


def _scan_row(table, table_name: str, row_filter: str | None) -> dict:
    stats = _plan_scan(table, row_filter)
    snapshot = table.current_snapshot()
    metrics = {
        "iceberg.scan.planning.duration_ns": stats["planning_duration_ns"],
        "iceberg.scan.data_files.result": stats["result_data_files"],
        "iceberg.scan.data_files.scanned": stats["scanned_data_files"],
        "iceberg.scan.data_files.skipped": stats["skipped_data_files"],
        "iceberg.scan.delete_files.result": stats["result_delete_files"],
        "iceberg.scan.data_manifests.total": stats["total_data_manifests"],
        "iceberg.scan.data_manifests.scanned": stats["scanned_data_manifests"],
        "iceberg.scan.data_manifests.skipped": stats["skipped_data_manifests"],
        "iceberg.scan.delete_manifests.total": stats["total_delete_manifests"],
        "iceberg.scan.records.result": stats["result_records"],
        "iceberg.scan.file_size_bytes.result": stats["result_file_size_bytes"],
    }
    row = _empty_row()
    row.update(
        {
            "report_id": str(uuid.uuid4()),
            "observed_at": datetime.now(timezone.utc),
            "report_type": "scan",
            "source": "scan-planning",
            "service_name": "iceberg-scan-planner",
            "table_name": table_name,
            "snapshot_id": None if snapshot is None else snapshot.snapshot_id,
            "sequence_number": None if snapshot is None else snapshot.sequence_number,
            "filter": row_filter,
            "duration_ns": stats["planning_duration_ns"],
            "result_data_files": stats["result_data_files"],
            "result_delete_files": stats["result_delete_files"],
            "total_data_manifests": stats["total_data_manifests"],
            "scanned_data_manifests": stats["scanned_data_manifests"],
            "skipped_data_manifests": stats["skipped_data_manifests"],
            "scanned_data_files": stats["scanned_data_files"],
            "skipped_data_files": stats["skipped_data_files"],
            "result_file_size_bytes": stats["result_file_size_bytes"],
            "attributes_json": json.dumps(
                {
                    "otel.scope.name": "iceberg.metrics",
                    "otel.scope.schema_url": "https://iceberg.apache.org/docs/latest/metrics-reporting/",
                    "iceberg.metadata.location": table.metadata_location,
                    "iceberg.table.location": table.location(),
                    "filter": row_filter or "",
                    "extracted_from": "scan-planning",
                },
                sort_keys=True,
            ),
            "metrics_json": json.dumps(metrics, sort_keys=True),
        }
    )
    return row


def _plan_scan(table, row_filter: str | None) -> dict:
    started = time.perf_counter_ns()
    scan = table.scan() if row_filter is None else table.scan(row_filter=row_filter)
    tasks = list(scan.plan_files())
    planning_duration_ns = time.perf_counter_ns() - started

    snapshot = table.current_snapshot()
    manifests = [] if snapshot is None else list(snapshot.manifests(table.io))
    data_manifests = [manifest for manifest in manifests if manifest.content == ManifestContent.DATA]
    delete_manifests = [manifest for manifest in manifests if manifest.content == ManifestContent.DELETES]
    scanned, skipped = _split_manifests(table, data_manifests, row_filter)

    delete_paths: set[str] = set()
    for task in tasks:
        for delete_file in task.delete_files:
            delete_paths.add(delete_file.file_path)

    files_in_scanned = sum(_live_files(manifest) for manifest in scanned)
    files_in_skipped = sum(_live_files(manifest) for manifest in skipped)
    result_data_files = len(tasks)
    # Files opened with a manifest, then dropped by partition or column stats.
    pruned_inside_manifests = max(0, files_in_scanned - result_data_files)

    return {
        "planning_duration_ns": planning_duration_ns,
        "result_data_files": result_data_files,
        "result_delete_files": len(delete_paths),
        "result_records": sum(task.file.record_count for task in tasks),
        "result_file_size_bytes": sum(task.file.file_size_in_bytes for task in tasks),
        "total_data_manifests": len(data_manifests),
        "scanned_data_manifests": len(scanned),
        "skipped_data_manifests": len(skipped),
        "scanned_data_files": files_in_scanned,
        "skipped_data_files": files_in_skipped + pruned_inside_manifests,
        "total_delete_manifests": len(delete_manifests),
    }


def _split_manifests(table, data_manifests, row_filter: str | None):
    if not row_filter:
        return list(data_manifests), []
    expression = parse(row_filter)
    schema = table.schema()
    specs = table.specs()
    evaluators = {}
    scanned = []
    skipped = []
    for manifest in data_manifests:
        spec_id = manifest.partition_spec_id
        evaluator = evaluators.get(spec_id)
        if evaluator is None:
            spec = specs[spec_id]
            projected = inclusive_projection(schema, spec, True)(expression)
            evaluator = manifest_evaluator(spec, schema, projected, True)
            evaluators[spec_id] = evaluator
        if evaluator(manifest):
            scanned.append(manifest)
        else:
            skipped.append(manifest)
    return scanned, skipped


def _live_files(manifest) -> int:
    return int(manifest.added_files_count or 0) + int(manifest.existing_files_count or 0)


def _empty_row() -> dict:
    return {field.name: None for field in REPORTS_ARROW}
