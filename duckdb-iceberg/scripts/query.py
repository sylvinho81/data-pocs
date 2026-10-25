#!/usr/bin/env python3
"""Run read queries against the Iceberg catalog (MinIO REST or Amazon S3 Tables).

Uses the same CONFIG_PATH / connect_catalog path as run_demo.py.

Examples
--------
Local MinIO (default config/local.yaml)::

    export PYTHONPATH=src
    python scripts/query.py
    python scripts/query.py --list-tables
    python scripts/query.py -q "SELECT id, metric_value FROM lake.poc.daily_metrics"

AWS S3 Tables::

    export PYTHONPATH=src
    export CONFIG_PATH=config/aws.yaml
    python scripts/query.py
    python scripts/query.py -q "SELECT count(*) FROM lake.playground.daily_metrics"
"""

from __future__ import annotations

import argparse
import sys

from duckdb_iceberg.config import load_settings
from duckdb_iceberg.session import connect_catalog


def _print_rel(rel) -> None:
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    if not rows:
        print("(0 rows)")
        if cols:
            print("  ".join(cols))
        return
    widths = [len(c) for c in cols]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    print("  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    for row in rows:
        print("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))
    print(f"({len(rows)} rows)")


def _default_sql(catalog: str, namespace: str, table: str, limit: int) -> str:
    return (
        f"SELECT * FROM {catalog}.{namespace}.{table} "
        f"ORDER BY 1 LIMIT {limit}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Query Iceberg tables via DuckDB (MinIO or S3 Tables)."
    )
    parser.add_argument(
        "-q",
        "--sql",
        help="SQL to run. Default: SELECT * from configured table.",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List attached tables (SHOW ALL TABLES) and exit.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Row limit for the default SELECT (default: 50).",
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    catalog = settings.iceberg.catalog_alias
    ns = settings.table.namespace
    table = settings.table.name
    fqn = f"{catalog}.{ns}.{table}"

    print(f"Config: {settings.name}")
    print(f"Warehouse: {settings.iceberg.attach_path}")
    print(f"REST/endpoint: {settings.iceberg.rest_uri or settings.iceberg.endpoint_type}")
    print(f"Default table: {fqn}")

    con = connect_catalog(settings)

    if args.list_tables:
        print("\n=== SHOW ALL TABLES ===")
        _print_rel(con.sql("SHOW ALL TABLES"))
        return 0

    sql = args.sql or _default_sql(catalog, ns, table, args.limit)
    print(f"\n=== SQL ===\n{sql}\n")
    try:
        _print_rel(con.sql(sql))
    except Exception as exc:  # noqa: BLE001 — surface DuckDB errors cleanly
        print(f"Query failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
