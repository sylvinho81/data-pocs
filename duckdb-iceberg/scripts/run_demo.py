#!/usr/bin/env python3
"""Create an Iceberg table on MinIO (via REST catalog), load seeds, and MERGE upserts by (id, event_date)."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import duckdb

from duckdb_iceberg.config import Settings, load_settings
from duckdb_iceberg.session import connect_catalog


def _load_seed_rows(path: Path) -> list[dict]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    for row in rows:
        row.pop("_note", None)
    return rows


def _print_frame(title: str, con, sql: str) -> None:
    print(f"\n=== {title} ===")
    rel = con.sql(sql)
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    widths = [len(c) for c in cols]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    print("  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    for row in rows:
        print("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def _set_merge_on_read_properties(con, fqn: str) -> None:
    con.execute(
        f"""
        CALL set_iceberg_table_properties({fqn}, {{
            'write.update.mode': 'merge-on-read',
            'write.delete.mode': 'merge-on-read'
        }});
        """
    )


def _try_create_table(con, fqn: str) -> bool:
    # Identity columns, not year()/month()/day() transforms. Transforms store
    # epoch offsets in the path (year_event_date=55) instead of calendar values.
    try:
        con.execute(
            f"""
            CREATE TABLE {fqn} (
                id BIGINT,
                event_date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                city VARCHAR,
                metric_name VARCHAR,
                metric_value DOUBLE,
                updated_at TIMESTAMP
            )
            PARTITIONED BY (year, month, day);
            """
        )
        return True
    except duckdb.Error:
        return False


def _table_columns(con, fqn: str) -> list[str] | None:
    try:
        rows = con.execute(f"DESCRIBE {fqn}").fetchall()
    except duckdb.Error:
        return None
    return [row[0] for row in rows]


def _ensure_table(con, settings: Settings) -> str:
    catalog = settings.iceberg.catalog_alias
    ns = settings.table.namespace
    table = settings.table.name
    fqn = f"{catalog}.{ns}.{table}"

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ns};")

    columns = _table_columns(con, fqn)
    if columns is not None and not {"year", "month", "day"}.issubset(columns):
        print(f"Dropping {fqn}: existing table is not partitioned by year/month/day.")
        con.execute(f"DROP TABLE {fqn}")
        columns = None

    if columns is None:
        if not _try_create_table(con, fqn):
            raise SystemExit(f"Could not create partitioned table {fqn}.")
        _set_merge_on_read_properties(con, fqn)
        return fqn

    # Already partitioned — clear rows for a clean demo (avoid fragile DROP against REST).
    try:
        con.execute(f"DELETE FROM {fqn}")
    except duckdb.Error as exc:
        raise SystemExit(
            f"Table {fqn} exists but is not usable ({exc}).\n"
            "Reset the stack with a fresh warehouse:\n"
            "  docker compose down -v && docker compose up --build"
        ) from exc

    _set_merge_on_read_properties(con, fqn)
    return fqn


def _ensure_staging(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE staging (
            id BIGINT,
            event_date DATE,
            city VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            updated_at TIMESTAMP
        );
        """
    )


def _insert_rows(con, fqn: str, rows: list[dict]) -> None:
    _ensure_staging(con)
    con.executemany(
        """
        INSERT INTO staging (id, event_date, city, metric_name, metric_value, updated_at)
        VALUES (?, ?::DATE, ?, ?, ?, ?::TIMESTAMP);
        """,
        [
            (
                row["id"],
                row["event_date"],
                row["city"],
                row["metric_name"],
                row["metric_value"],
                row["updated_at"],
            )
            for row in rows
        ],
    )
    con.execute(
        f"""
        INSERT INTO {fqn} (
            id, event_date, year, month, day, city, metric_name, metric_value, updated_at
        )
        SELECT
            id,
            event_date,
            year(event_date),
            month(event_date),
            day(event_date),
            city,
            metric_name,
            metric_value,
            updated_at
        FROM staging;
        """
    )


def _merge_upsert(con, fqn: str, rows: list[dict]) -> None:
    _ensure_staging(con)
    con.execute("DELETE FROM staging;")
    con.executemany(
        """
        INSERT INTO staging (id, event_date, city, metric_name, metric_value, updated_at)
        VALUES (?, ?::DATE, ?, ?, ?, ?::TIMESTAMP);
        """,
        [
            (
                row["id"],
                row["event_date"],
                row["city"],
                row["metric_name"],
                row["metric_value"],
                row["updated_at"],
            )
            for row in rows
        ],
    )
    con.execute(
        f"""
        MERGE INTO {fqn} AS target
        USING staging AS source
        ON target.id = source.id AND target.event_date = source.event_date
        WHEN MATCHED THEN UPDATE SET
            year = year(source.event_date),
            month = month(source.event_date),
            day = day(source.event_date),
            city = source.city,
            metric_name = source.metric_name,
            metric_value = source.metric_value,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, event_date, year, month, day, city, metric_name, metric_value, updated_at
        ) VALUES (
            source.id, source.event_date,
            year(source.event_date), month(source.event_date), day(source.event_date),
            source.city, source.metric_name, source.metric_value, source.updated_at
        );
        """
    )


def main() -> int:
    settings = load_settings()
    seeds_dir = settings.repo_root / "seeds"
    initial_path = seeds_dir / "initial_metrics.json"
    upsert_path = seeds_dir / "upsert_batch.json"

    print(f"Config: {settings.name}")
    print(f"Warehouse: {settings.iceberg.attach_path}")
    print(f"REST: {settings.iceberg.rest_uri or settings.iceberg.endpoint_type}")
    print(f"Table: {settings.table.qualified}")

    # Brief settle after compose healthcheck (REST JVM can still be warming).
    time.sleep(1)

    con = connect_catalog(settings)
    fqn = _ensure_table(con, settings)

    initial_rows = _load_seed_rows(initial_path)
    upsert_rows = _load_seed_rows(upsert_path)

    print(f"\nLoading {len(initial_rows)} seed rows from {initial_path.name} ...")
    _insert_rows(con, fqn, initial_rows)
    _print_frame("After initial load", con, f"SELECT * FROM {fqn} ORDER BY id, event_date")

    print(f"\nApplying MERGE upsert batch ({len(upsert_rows)} rows) from {upsert_path.name} ...")
    _merge_upsert(con, fqn, upsert_rows)
    _print_frame("After MERGE upsert", con, f"SELECT * FROM {fqn} ORDER BY id, event_date")

    _print_frame(
        "Upsert checks (expected: Madrid 2025-10-01 value=1300, row count=5)",
        con,
        f"""
        SELECT
            count(*) AS row_count,
            max(CASE WHEN id = 1001 AND event_date = DATE '2025-10-01' THEN metric_value END) AS madrid_oct1_value,
            max(CASE WHEN id = 1004 AND event_date = DATE '2025-10-02' THEN city END) AS seville_city
        FROM {fqn};
        """,
    )

    _print_frame(
        "Partitions (year / month / day of event_date)",
        con,
        f"""
        SELECT year, month, day, count(*) AS rows
        FROM {fqn}
        GROUP BY year, month, day
        ORDER BY year, month, day;
        """,
    )

    _print_frame(
        "Data files (path should contain year=/month=/day=)",
        con,
        f"""
        SELECT file_path, record_count, content
        FROM iceberg_metadata({fqn})
        ORDER BY file_path;
        """,
    )

    snap_count = con.sql(f"SELECT count(*) FROM iceberg_snapshots({fqn})").fetchone()[0]
    print(f"\nIceberg snapshots on {fqn}: {snap_count}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
