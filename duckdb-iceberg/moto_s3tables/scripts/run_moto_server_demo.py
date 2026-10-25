#!/usr/bin/env python3
"""Same S3 Tables control-plane demo against a standing ``moto_server`` process.

Terminal 1::

    moto_server -p 5001

Terminal 2::

    MOTO_ENDPOINT=http://127.0.0.1:5001 python scripts/run_moto_server_demo.py

Useful to inspect HTTP traffic or share one mock AWS account across processes.
Docs: https://docs.getmoto.org/en/latest/docs/server_mode.html
"""

from __future__ import annotations

import os
import sys

import boto3

REGION = os.getenv("MOTO_REGION", "us-east-1")
ENDPOINT = os.getenv("MOTO_ENDPOINT", "http://127.0.0.1:5001")
BUCKET_NAME = os.getenv("S3_TABLES_BUCKET", "poc-table-bucket")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "poc")
TABLE = os.getenv("ICEBERG_TABLE", "daily_metrics")


def main() -> int:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_REGION"] = REGION

    client = boto3.client("s3tables", region_name=REGION, endpoint_url=ENDPOINT)

    print(f"Using moto_server at {ENDPOINT}")
    arn = client.create_table_bucket(name=BUCKET_NAME)["arn"]
    print(f"create_table_bucket → {arn}")

    # Idempotent-ish: ignore if namespace/table already exist from a previous run.
    try:
        client.create_namespace(tableBucketARN=arn, namespace=[NAMESPACE])
        print(f"create_namespace → {NAMESPACE}")
    except Exception as exc:  # noqa: BLE001 — Conflict if re-run
        print(f"create_namespace: {exc}")

    try:
        created = client.create_table(
            tableBucketARN=arn,
            namespace=NAMESPACE,
            name=TABLE,
            format="ICEBERG",
        )
        print(f"create_table → {created['tableARN']}")
    except Exception as exc:  # noqa: BLE001
        print(f"create_table skipped/failed: {exc}")

    print("list_tables →", [t["name"] for t in client.list_tables(tableBucketARN=arn, namespace=NAMESPACE)["tables"]])
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
