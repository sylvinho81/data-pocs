#!/usr/bin/env python3
"""Exercise Amazon S3 Tables APIs against [moto](https://github.com/getmoto/moto).

What this proves
----------------
Moto implements the S3 Tables *control plane* (create table bucket, namespace, table,
list/get). That is the same boto3 ``s3tables`` client you would use against real AWS.

What this does *not* prove
--------------------------
DuckDB ``ATTACH ... (TYPE iceberg, ENDPOINT_TYPE s3_tables)`` talks to the AWS
**Iceberg REST** facade (``https://s3tables.<region>.amazonaws.com/iceberg``), not the
raw ``s3tables`` management API. Moto does not implement that Iceberg REST surface yet
(see https://docs.getmoto.org/en/stable/docs/services/s3tables.html), so DuckDB cannot
complete the same write/MERGE path against moto that it can against real S3 Tables or
the MinIO + Iceberg REST stack in the parent folder.

Run
---
From this directory (or repo root with PYTHONPATH unused — script is standalone):

    pip install -r requirements.txt
    python scripts/run_moto_demo.py

Optional: also try DuckDB attach (expected to fail against moto):

    TRY_DUCKDB=1 python scripts/run_moto_demo.py
"""

from __future__ import annotations

import os
import sys

import boto3
from moto import mock_aws

# Moto's s3tables backend is registered for us-east-1; picking up AWS_REGION=eu-west-1
# from a real AWS profile causes KeyError inside moto. Override only via MOTO_REGION.
REGION = os.getenv("MOTO_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_TABLES_BUCKET", "poc-table-bucket")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "poc")
TABLE = os.getenv("ICEBERG_TABLE", "daily_metrics")


def run_s3tables_control_plane() -> str:
    client = boto3.client("s3tables", region_name=REGION)

    print(f"=== Moto S3 Tables control plane (region={REGION}) ===")
    if os.getenv("AWS_REGION") and os.getenv("AWS_REGION") != REGION:
        print(
            f"(ignoring AWS_REGION={os.getenv('AWS_REGION')}; "
            f"use MOTO_REGION to override the mock region)"
        )
    arn = client.create_table_bucket(name=BUCKET_NAME)["arn"]
    print(f"create_table_bucket → {arn}")

    client.create_namespace(tableBucketARN=arn, namespace=[NAMESPACE])
    print(f"create_namespace → {NAMESPACE}")

    created = client.create_table(
        tableBucketARN=arn,
        namespace=NAMESPACE,
        name=TABLE,
        format="ICEBERG",
    )
    print(f"create_table → {created['tableARN']}")

    tables = client.list_tables(tableBucketARN=arn, namespace=NAMESPACE)["tables"]
    print(f"list_tables → {[t['name'] for t in tables]}")

    meta = client.get_table(tableBucketARN=arn, namespace=NAMESPACE, name=TABLE)
    print(f"get_table.format → {meta.get('format')}")
    print(f"get_table.warehouseLocation → {meta.get('warehouseLocation')}")

    try:
        loc = client.get_table_metadata_location(
            tableBucketARN=arn, namespace=NAMESPACE, name=TABLE
        )
        print(f"get_table_metadata_location → {loc}")
    except Exception as exc:  # noqa: BLE001 — demo: show moto gaps
        print(f"get_table_metadata_location → not usable yet ({type(exc).__name__}: {exc})")

    return arn


def try_duckdb_attach(table_bucket_arn: str) -> None:
    import duckdb

    print("\n=== DuckDB ATTACH (ENDPOINT_TYPE s3_tables) against real AWS endpoint ===")
    print("DuckDB always targets s3tables.<region>.amazonaws.com/iceberg for this mode.")
    print("With moto only (no AWS creds / no real bucket), expect Forbidden or similar.\n")

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute(
        """
        CREATE OR REPLACE SECRET s3_storage (
            TYPE s3,
            PROVIDER credential_chain,
            REGION $region
        );
        """,
        {"region": REGION},
    )
    try:
        con.execute(
            f"""
            ATTACH '{table_bucket_arn}' AS lake (
                TYPE iceberg,
                ENDPOINT_TYPE 'S3_TABLES',
                READ_ONLY false
            );
            """
        )
        print("ATTACH succeeded unexpectedly — listing tables:")
        print(con.execute("SHOW ALL TABLES").fetchall())
    except Exception as exc:  # noqa: BLE001
        print(f"ATTACH failed as expected for local-only moto: {exc}")


def main() -> int:
    # Dummy creds so boto3 is happy inside mock_aws (moto ignores signature).
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    # Force mock region so a leftover AWS_REGION=eu-west-1 does not break moto.
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_REGION"] = REGION

    with mock_aws():
        arn = run_s3tables_control_plane()
        print("\nMoto control-plane demo OK.")
        print(
            "Next for full DuckDB MERGE: use parent MinIO+REST POC, or real AWS "
            "(config/aws_s3_tables.example.yaml)."
        )

        if os.getenv("TRY_DUCKDB", "").lower() in {"1", "true", "yes"}:
            # Leave mock context: DuckDB will hit real AWS HTTPS, not moto.
            pass

    if os.getenv("TRY_DUCKDB", "").lower() in {"1", "true", "yes"}:
        # Use a placeholder ARN shaped like moto's — DuckDB still calls AWS.
        try_duckdb_attach(
            f"arn:aws:s3tables:{REGION}:123456789012:bucket/{BUCKET_NAME}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
