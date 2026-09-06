#!/usr/bin/env python3
"""Verify Iceberg tables written by the Flink job (via REST catalog + MinIO)."""

from __future__ import annotations

import os
import sys

from pyiceberg.catalog import load_catalog


def main() -> int:
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:18181")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:19000")

    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": rest_uri,
            "warehouse": warehouse,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    namespaces = catalog.list_namespaces()
    print(f"Namespaces: {namespaces}")

    tables = []
    for ns in namespaces:
        tables.extend(catalog.list_tables(ns))
    print(f"Tables: {tables}")

    raw_name = ("earthquakes", "earthquakes_raw")
    agg_name = ("earthquakes", "earthquakes_by_minute")

    if raw_name not in tables and ("earthquakes",) not in [t[:1] for t in tables]:
        # list_tables may return identifiers differently depending on pyiceberg version
        print("Looking up tables under namespace 'earthquakes' ...")
        try:
            tables = catalog.list_tables("earthquakes")
            print(f"Tables in earthquakes: {tables}")
        except Exception as exc:
            print(f"Could not list earthquakes tables yet: {exc}")
            print("Is the Flink job running and has it completed a checkpoint?")
            return 1

    def show_table(identifier) -> None:
        table = catalog.load_table(identifier)
        snapshots = list(table.snapshots())
        print(f"\n=== {identifier} snapshots={len(snapshots)} ===")
        scan = table.scan().to_arrow()
        print(f"rows={scan.num_rows}")
        if scan.num_rows:
            print(scan.slice(0, min(10, scan.num_rows)).to_pandas().to_string(index=False))

    try:
        show_table("earthquakes.earthquakes_raw")
    except Exception as exc:
        print(f"earthquakes_raw not ready: {exc}")
        return 1

    try:
        show_table("earthquakes.earthquakes_by_minute")
    except Exception as exc:
        print(f"earthquakes_by_minute not ready yet (windows may still be open): {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
