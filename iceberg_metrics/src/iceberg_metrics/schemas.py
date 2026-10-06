"""Iceberg and Arrow schemas for the event seeds and the metrics sink."""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

EVENTS_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_time", TimestamptzType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "source", StringType(), required=True),
    NestedField(5, "city", StringType(), required=True),
    NestedField(6, "metric_value", DoubleType(), required=True),
    NestedField(7, "unit", StringType(), required=True),
    NestedField(8, "payload_json", StringType(), required=True),
    NestedField(9, "seed_uri", StringType(), required=True),
)

# Identity on city so a city filter can skip whole manifests during scan planning.
EVENTS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=IdentityTransform(), name="city")
)

EVENTS_ARROW = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("city", pa.string(), nullable=False),
        pa.field("metric_value", pa.float64(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
        pa.field("seed_uri", pa.string(), nullable=False),
    ]
)

# One row per CommitReport or ScanReport. Commit rows fill the snapshot
# counters; scan rows fill the planning counters. metrics_json keeps the
# full OpenTelemetry-style map either way.
REPORTS_SCHEMA = Schema(
    NestedField(1, "report_id", StringType(), required=True),
    NestedField(2, "observed_at", TimestamptzType(), required=True),
    NestedField(3, "report_type", StringType(), required=True),
    NestedField(4, "source", StringType(), required=True),
    NestedField(5, "service_name", StringType(), required=True),
    NestedField(6, "table_name", StringType(), required=True),
    NestedField(7, "snapshot_id", LongType(), required=False),
    NestedField(8, "sequence_number", LongType(), required=False),
    NestedField(9, "operation", StringType(), required=False),
    NestedField(10, "filter", StringType(), required=False),
    NestedField(11, "duration_ns", LongType(), required=False),
    NestedField(12, "attempts", LongType(), required=False),
    NestedField(13, "added_data_files", LongType(), required=False),
    NestedField(14, "removed_data_files", LongType(), required=False),
    NestedField(15, "total_data_files", LongType(), required=False),
    NestedField(16, "added_records", LongType(), required=False),
    NestedField(17, "removed_records", LongType(), required=False),
    NestedField(18, "total_records", LongType(), required=False),
    NestedField(19, "added_files_size_bytes", LongType(), required=False),
    NestedField(20, "removed_files_size_bytes", LongType(), required=False),
    NestedField(21, "total_files_size_bytes", LongType(), required=False),
    NestedField(22, "added_delete_files", LongType(), required=False),
    NestedField(23, "removed_delete_files", LongType(), required=False),
    NestedField(24, "total_delete_files", LongType(), required=False),
    NestedField(25, "added_positional_deletes", LongType(), required=False),
    NestedField(26, "removed_positional_deletes", LongType(), required=False),
    NestedField(27, "total_positional_deletes", LongType(), required=False),
    NestedField(28, "added_equality_deletes", LongType(), required=False),
    NestedField(29, "removed_equality_deletes", LongType(), required=False),
    NestedField(30, "total_equality_deletes", LongType(), required=False),
    NestedField(31, "result_data_files", LongType(), required=False),
    NestedField(32, "result_delete_files", LongType(), required=False),
    NestedField(33, "total_data_manifests", LongType(), required=False),
    NestedField(34, "scanned_data_manifests", LongType(), required=False),
    NestedField(35, "skipped_data_manifests", LongType(), required=False),
    NestedField(36, "scanned_data_files", LongType(), required=False),
    NestedField(37, "skipped_data_files", LongType(), required=False),
    NestedField(38, "result_file_size_bytes", LongType(), required=False),
    NestedField(39, "attributes_json", StringType(), required=True),
    NestedField(40, "metrics_json", StringType(), required=True),
)

_LONG = pa.int64()
REPORTS_ARROW = pa.schema(
    [
        pa.field("report_id", pa.string(), nullable=False),
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("report_type", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("service_name", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("snapshot_id", _LONG),
        pa.field("sequence_number", _LONG),
        pa.field("operation", pa.string()),
        pa.field("filter", pa.string()),
        pa.field("duration_ns", _LONG),
        pa.field("attempts", _LONG),
        pa.field("added_data_files", _LONG),
        pa.field("removed_data_files", _LONG),
        pa.field("total_data_files", _LONG),
        pa.field("added_records", _LONG),
        pa.field("removed_records", _LONG),
        pa.field("total_records", _LONG),
        pa.field("added_files_size_bytes", _LONG),
        pa.field("removed_files_size_bytes", _LONG),
        pa.field("total_files_size_bytes", _LONG),
        pa.field("added_delete_files", _LONG),
        pa.field("removed_delete_files", _LONG),
        pa.field("total_delete_files", _LONG),
        pa.field("added_positional_deletes", _LONG),
        pa.field("removed_positional_deletes", _LONG),
        pa.field("total_positional_deletes", _LONG),
        pa.field("added_equality_deletes", _LONG),
        pa.field("removed_equality_deletes", _LONG),
        pa.field("total_equality_deletes", _LONG),
        pa.field("result_data_files", _LONG),
        pa.field("result_delete_files", _LONG),
        pa.field("total_data_manifests", _LONG),
        pa.field("scanned_data_manifests", _LONG),
        pa.field("skipped_data_manifests", _LONG),
        pa.field("scanned_data_files", _LONG),
        pa.field("skipped_data_files", _LONG),
        pa.field("result_file_size_bytes", _LONG),
        pa.field("attributes_json", pa.string(), nullable=False),
        pa.field("metrics_json", pa.string(), nullable=False),
    ]
)
