#!/usr/bin/env python3
"""Load NYC yellow taxi parquet into ClickHouse using Polars."""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path

import clickhouse_connect
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET = ROOT / "files" / "yellow_tripdata_2025-11.parquet"
SEED_PATH = ROOT / "nyc_taxi" / "seeds" / "random_sample_location_ids.csv"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "nyc_taxi"
CLICKHOUSE_TABLE = "yellow_trip"

COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "payment_type",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "Airport_fee",
    "cbd_congestion_fee",
]

RENAMES = {
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "payment_type": "payment_type",
    "total_amount": "total_amount",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


def _client(database: str | None = None):
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="",
        database=database or "default",
    )


def create_database_and_table(recreate: bool = False) -> None:
    client = _client()
    client.command(f"create database if not exists {CLICKHOUSE_DATABASE}")
    client.database = CLICKHOUSE_DATABASE

    if recreate:
        client.command(f"drop table if exists {CLICKHOUSE_TABLE}")

    client.command(
        f"""
        create table if not exists {CLICKHOUSE_TABLE} (
            tpep_pickup_datetime DateTime,
            tpep_dropoff_datetime DateTime,
            passenger_count Nullable(Int64),
            payment_type Nullable(Int64),
            total_amount Float64,
            pu_location_id Int32,
            do_location_id Int32,
            airport_fee Float64,
            cbd_congestion_fee Float64
        )
        engine = MergeTree()
        order by (tpep_pickup_datetime, pu_location_id)
        """
    )


def write_random_location_seed(
    parquet_path: Path,
    num_samples: int = 50,
    seed: int = 42,
) -> list[int]:
    """Mirror benchmark.py random-access sampling with a fixed seed."""
    location_ids = (
        pl.scan_parquet(parquet_path)
        .select("PULocationID")
        .unique()
        .collect()["PULocationID"]
        .to_list()
    )
    rng = random.Random(seed)
    sample_size = min(num_samples, len(location_ids))
    sampled = (
        rng.sample(location_ids, sample_size)
        if len(location_ids) > sample_size
        else location_ids
    )

    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SEED_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["location_id"])
        for location_id in sorted(sampled):
            writer.writerow([location_id])

    return sampled


def load_parquet(
    parquet_path: Path,
    insert_batch_size: int = 200_000,
    recreate: bool = False,
) -> int:
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    create_database_and_table(recreate=recreate)
    client = _client(CLICKHOUSE_DATABASE)

    df = pl.read_parquet(parquet_path).select(COLUMNS).rename(RENAMES)
    total_rows = len(df)

    for start in range(0, total_rows, insert_batch_size):
        chunk = df.slice(start, insert_batch_size)
        client.insert_arrow(CLICKHOUSE_TABLE, chunk.to_arrow())

    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet",
        type=Path,
        default=DEFAULT_PARQUET,
        help="Path to yellow taxi parquet file",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop and recreate the raw table before loading",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200_000,
        help="Insert batch size",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(f"Generating random location seed from {args.parquet} ...")
    sampled = write_random_location_seed(args.parquet)
    print(f"  Wrote {len(sampled)} location IDs to {SEED_PATH}")

    print(f"Loading parquet into ClickHouse ({CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}) ...")
    rows = load_parquet(args.parquet, insert_batch_size=args.batch_size, recreate=args.recreate)
    print(f"  Loaded {rows:,} rows.")


if __name__ == "__main__":
    main()
