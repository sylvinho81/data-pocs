from __future__ import annotations

import textwrap
from dataclasses import dataclass

import polars as pl
import psycopg2
from psycopg2.extras import execute_batch

from settings import RAW_TAXI_ZONE_LOOKUP, PROJECT_ROOT, POSTGRES_DSN


@dataclass
class LookupTable:
    name: str
    id_column: str
    value_column: str


LOOKUP_TABLES: list[LookupTable] = [
    LookupTable("vendor", "id", "description"),
    LookupTable("rate_code", "id", "description"),
    LookupTable("payment_type", "id", "description"),
]


def _connect():
    return psycopg2.connect(POSTGRES_DSN)


def create_lookup_schema() -> None:
    ddl = textwrap.dedent(
        """
        create schema if not exists dim;

        create table if not exists dim.vendor (
            id integer primary key,
            description text not null
        );

        create table if not exists dim.rate_code (
            id integer primary key,
            description text not null
        );

        create table if not exists dim.payment_type (
            id integer primary key,
            description text not null
        );

        create table if not exists dim.taxi_zone (
            location_id integer primary key,
            borough text,
            zone text,
            service_zone text
        );
        """
    )
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(ddl)


def load_static_lookups() -> None:
    vendor_rows = [
        (1, "Creative Mobile Technologies"),
        (2, "VeriFone Inc"),
    ]

    rate_code_rows = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride"),
        (99, "Null/unknown"),
    ]

    payment_type_rows = [
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ]

    with _connect() as conn, conn.cursor() as cur:
        execute_batch(
            cur,
            "insert into dim.vendor (id, description) values (%s, %s) "
            "on conflict (id) do update set description = excluded.description",
            vendor_rows,
        )
        execute_batch(
            cur,
            "insert into dim.rate_code (id, description) values (%s, %s) "
            "on conflict (id) do update set description = excluded.description",
            rate_code_rows,
        )
        execute_batch(
            cur,
            "insert into dim.payment_type (id, description) values (%s, %s) "
            "on conflict (id) do update set description = excluded.description",
            payment_type_rows,
        )


def load_taxi_zone_lookup() -> None:
    df = pl.read_csv(RAW_TAXI_ZONE_LOOKUP)
    df = df.rename(
        {
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    )

    records = list(df.iter_rows())

    with _connect() as conn, conn.cursor() as cur:
        execute_batch(
            cur,
            """
            insert into dim.taxi_zone (location_id, borough, zone, service_zone)
            values (%s, %s, %s, %s)
            on conflict (location_id) do update
            set borough = excluded.borough,
                zone = excluded.zone,
                service_zone = excluded.service_zone
            """,
            records,
        )


def main() -> None:
    print("Creating lookup schema in Postgres...")
    create_lookup_schema()
    print("Loading static lookup tables...")
    load_static_lookups()
    print("Loading taxi zone lookup CSV...")
    load_taxi_zone_lookup()
    print("Postgres lookup loading completed.")


if __name__ == "__main__":
    main()

