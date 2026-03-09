from __future__ import annotations

import polars as pl
import psycopg2
import clickhouse_connect

from settings import (
    RAW_PARQUET_DIR,
    POSTGRES_DSN,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USERNAME,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)


def _pg_conn():
    return psycopg2.connect(POSTGRES_DSN)


def _ch_client(database: str | None = None):
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD,
        database=database or CLICKHOUSE_DATABASE,
    )


def create_star_schema() -> None:
    # Connect to default DB first (taxi may not exist yet)
    client_default = _ch_client(database="default")
    client_default.command(f"create database if not exists {CLICKHOUSE_DATABASE}")
    client_default.close()

    client = _ch_client()

    client.command(
        """
        create table if not exists dim_vendor (
            id Int32,
            description String
        ) engine = MergeTree
        order by id
        """
    )
    client.command(
        """
        create table if not exists dim_rate_code (
            id Int32,
            description String
        ) engine = MergeTree
        order by id
        """
    )
    client.command(
        """
        create table if not exists dim_payment_type (
            id Int32,
            description String
        ) engine = MergeTree
        order by id
        """
    )
    client.command(
        """
        create table if not exists dim_taxi_zone (
            location_id Int32,
            borough String,
            zone String,
            service_zone String
        ) engine = MergeTree
        order by location_id
        """
    )

    client.command(
        """
        create table if not exists fact_yellow_trip (
            trip_id UInt64,
            vendor_id Int32,
            rate_code_id Int32,
            payment_type Int32,
            pickup_datetime DateTime,
            dropoff_datetime DateTime,
            passenger_count Int32,
            trip_distance Float64,
            fare_amount Float64,
            tip_amount Float64,
            total_amount Float64,
            pulocation_id Int32,
            dolocation_id Int32
        ) engine = MergeTree
        order by (pickup_datetime, pulocation_id)
        """
    )


def load_dimensions_from_postgres() -> None:
    client = _ch_client()

    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("select id, description from dim.vendor")
        vendor_rows = cur.fetchall()
        cur.execute("select id, description from dim.rate_code")
        rate_rows = cur.fetchall()
        cur.execute("select id, description from dim.payment_type")
        payment_rows = cur.fetchall()
        cur.execute(
            "select location_id, borough, zone, service_zone from dim.taxi_zone"
        )
        zone_rows = cur.fetchall()

    client.insert("dim_vendor", vendor_rows, column_names=["id", "description"])
    client.insert("dim_rate_code", rate_rows, column_names=["id", "description"])
    client.insert(
        "dim_payment_type", payment_rows, column_names=["id", "description"]
    )
    client.insert(
        "dim_taxi_zone",
        zone_rows,
        column_names=["location_id", "borough", "zone", "service_zone"],
    )


def load_fact_from_parquet(
    limit_rows: int = 1_000_000,
    insert_batch_size: int = 100_000,
) -> None:
    """Load fact table from parquet in batches to avoid OOM (file-by-file, then chunked inserts)."""
    cols = [
        "VendorID",
        "RatecodeID",
        "payment_type",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ]
    renames = {
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "payment_type": "payment_type",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pulocation_id",
        "DOLocationID": "dolocation_id",
    }
    parquet_files = sorted(RAW_PARQUET_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files in {RAW_PARQUET_DIR}")

    client = _ch_client()
    total_loaded = 0
    next_trip_id = 1

    for path in parquet_files:
        if total_loaded >= limit_rows:
            break
        need = limit_rows - total_loaded
        df = pl.read_parquet(path)
        available = [c for c in cols if c in df.columns]
        if not available:
            continue
        df = df.select(available).head(need)
        if df.is_empty():
            continue
        df = df.rename(renames)
        df = df.with_row_count("trip_id", offset=next_trip_id).with_columns(
            pl.col("trip_id").cast(pl.UInt64)
        )
        n = len(df)
        next_trip_id += n
        total_loaded += n

        for start in range(0, n, insert_batch_size):
            chunk = df.slice(start, insert_batch_size)
            arrow_table = chunk.to_arrow()
            client.insert_arrow("fact_yellow_trip", arrow_table)
        del df

    print(f"  Inserted {total_loaded} rows into fact_yellow_trip.")


def main() -> None:
    print("Creating ClickHouse star schema...")
    create_star_schema()
    print("Loading dimensions from Postgres into ClickHouse...")
    load_dimensions_from_postgres()
    print("Loading fact_yellow_trip from parquet into ClickHouse...")
    load_fact_from_parquet()
    print("ClickHouse star schema population completed.")


if __name__ == "__main__":
    main()

