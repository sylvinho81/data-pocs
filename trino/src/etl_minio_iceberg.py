from __future__ import annotations

from pathlib import Path

import polars as pl
from minio import Minio

from settings import (
    RAW_PARQUET_DIR,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_RAW_BUCKET,
)


def _minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_raw_parquet() -> None:
    raw_dir = Path(RAW_PARQUET_DIR)
    if not raw_dir.is_dir():
        raise SystemExit(
            f"Raw parquet directory not found: {raw_dir}\n"
            f"Put NYC taxi parquet files in benchmark_vortex_parquet/ny_taxi_files/ or set RAW_PARQUET_DIR in settings.py."
        )
    paths = sorted(raw_dir.glob("*.parquet"))
    if not paths:
        raise SystemExit(
            f"No *.parquet files in {raw_dir}\n"
            f"Add parquet files there (e.g. from benchmark_vortex_parquet) and re-run."
        )

    client = _minio_client()
    _ensure_bucket(client, MINIO_RAW_BUCKET)

    for path in paths:
        object_name = f"ny_taxi_raw/{path.name}"
        print(f"Uploading raw parquet {path.name} -> {MINIO_RAW_BUCKET}/{object_name}")
        client.fput_object(MINIO_RAW_BUCKET, object_name, str(path))


def main() -> None:
    print("Uploading raw parquet files to MinIO (taxi-raw bucket)...")
    upload_raw_parquet()
    print(
        "Raw upload done. Silver layer is Apache Iceberg: run sql/01_setup_iceberg_silver.sql "
        "in Trino to create the Iceberg table and load it from raw."
    )


if __name__ == "__main__":
    main()

