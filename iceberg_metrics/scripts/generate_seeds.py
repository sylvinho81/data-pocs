#!/usr/bin/env python3
"""Write one JSON seed per event, then commit them in random-sized batches.

Each batch is one Iceberg snapshot. ``added-records`` is the batch size.
``added-data-files`` is the number of distinct cities in the batch, because
the table is partitioned by city.

    python scripts/generate_seeds.py
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from iceberg_metrics.catalog import load_hive_catalog, recreate_table, warehouse_uri, write_bytes
from iceberg_metrics.schemas import EVENTS_ARROW, EVENTS_PARTITION_SPEC, EVENTS_SCHEMA

CITIES = ("Madrid", "Barcelona", "Valencia", "Bilbao")
EVENT_TYPES = (
    ("air.quality", "ug/m3", 5.0, 80.0),
    ("traffic.flow", "vehicles", 10.0, 400.0),
    ("energy.usage", "kwh", 0.2, 12.0),
    ("noise.level", "db", 35.0, 90.0),
)


def main() -> int:
    count = int(os.getenv("SEED_COUNT", "12"))
    namespace = os.getenv("SEED_NAMESPACE", "events")
    table_name = os.getenv("SEED_TABLE", "seeds")
    identifier = f"{namespace}.{table_name}"
    service_name = os.getenv("OTEL_SERVICE_NAME", "iceberg-seed-writer")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]

    catalog = load_hive_catalog()
    table = recreate_table(
        catalog,
        identifier,
        EVENTS_SCHEMA,
        partition_spec=EVENTS_PARTITION_SPEC,
    )

    events = [build_event(index, count, run_id) for index in range(count)]
    for event in events:
        payload = json.dumps(event["document"], indent=2).encode() + b"\n"
        write_bytes(table.io, event["seed_uri"], payload)

    max_batch = int(os.getenv("SEED_BATCH_MAX", "4"))
    batches = random_batches(events, max_batch)
    sizes = [len(batch) for batch in batches]
    print(f"wrote {count} JSON seeds, run {run_id}")
    print(f"committing them in {len(batches)} random batches: {sizes}")
    for batch_index, batch in enumerate(batches, start=1):
        attempts = append_batch(table, batch, service_name)
        snapshot = table.current_snapshot()
        cities = ",".join(event["city"] for event in batch)
        print(
            f"  batch {batch_index}  rows={len(batch)}  cities={cities}  "
            f"snapshot={snapshot.snapshot_id}  attempts={attempts}"
        )

    table = catalog.load_table(identifier)
    print()
    print(f"table location:    {table.location()}")
    print(f"metadata file:     {table.metadata_location}")
    print(f"snapshots:         {len(table.snapshots())}")
    print("each snapshot summary in that metadata file holds added-data-files,")
    print("added-records, added-files-size, and the running totals.")
    return 0


def build_event(index: int, count: int, run_id: str) -> dict:
    city = CITIES[index % len(CITIES)]
    event_type, unit, low, high = EVENT_TYPES[index % len(EVENT_TYPES)]
    span = high - low
    value = round(low + span * ((index * 37 % 100) / 100), 3)
    event_id = f"evt-{run_id}-{index + 1:04d}"
    event_time = datetime.now(timezone.utc) - timedelta(minutes=count - index)
    document = {
        "event_id": event_id,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_type": event_type,
        "source": f"sensor-{city[:3].lower()}-{index % 3}",
        "city": city,
        "value": value,
        "unit": unit,
    }
    seed_uri = warehouse_uri("raw-seeds", run_id, f"{event_id}.json")
    return {
        "event_id": event_id,
        "event_time": event_time,
        "event_type": event_type,
        "source": document["source"],
        "city": city,
        "metric_value": value,
        "unit": unit,
        "payload_json": json.dumps(document, indent=2),
        "seed_uri": seed_uri,
        "document": document,
    }


def random_batches(events: list[dict], max_batch: int) -> list[list[dict]]:
    """Split events into batches of size 1..max_batch until none remain."""
    if max_batch < 1:
        raise ValueError("SEED_BATCH_MAX must be at least 1")
    pending = list(events)
    batches: list[list[dict]] = []
    while pending:
        size = random.randint(1, min(max_batch, len(pending)))
        batches.append(pending[:size])
        pending = pending[size:]
    return batches


def append_batch(table, batch: list[dict], service_name: str) -> int:
    """Commit one snapshot for the batch. One Parquet file is written per city."""
    arrow = pa.table(
        {
            "event_id": pa.array([event["event_id"] for event in batch], type=pa.string()),
            "event_time": pa.array([event["event_time"] for event in batch], type=pa.timestamp("us", tz="UTC")),
            "event_type": pa.array([event["event_type"] for event in batch], type=pa.string()),
            "source": pa.array([event["source"] for event in batch], type=pa.string()),
            "city": pa.array([event["city"] for event in batch], type=pa.string()),
            "metric_value": pa.array([event["metric_value"] for event in batch], type=pa.float64()),
            "unit": pa.array([event["unit"] for event in batch], type=pa.string()),
            "payload_json": pa.array([event["payload_json"] for event in batch], type=pa.string()),
            "seed_uri": pa.array([event["seed_uri"] for event in batch], type=pa.string()),
        },
        schema=EVENTS_ARROW,
    )
    event_ids = ",".join(event["event_id"] for event in batch)
    attempts = 0
    while True:
        attempts += 1
        started_ms = int(time.time() * 1000)
        try:
            table.append(
                arrow,
                snapshot_properties={
                    "commit-started-at-epoch-ms": str(started_ms),
                    "commit-attempts": str(attempts),
                    "otel.service.name": service_name,
                    "seed.batch-size": str(len(batch)),
                    "seed.event-ids": event_ids,
                },
            )
            return attempts
        except CommitFailedException:
            if attempts >= 5:
                raise
            print(f"  commit conflict on batch [{event_ids}], attempt {attempts}")
            table.refresh()
            time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(main())
