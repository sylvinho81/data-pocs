#!/usr/bin/env bash
# Run the upsert demo against MinIO + Iceberg REST.
set -euo pipefail
cd "$(dirname "$0")/.."

# Drop any half-broken REST container from the old stop/wipe flow.
docker compose down --remove-orphans >/dev/null 2>&1 || true
docker rm -f duckdb-iceberg-rest duckdb-iceberg-wait-rest duckdb-iceberg-rest-stop \
  duckdb-iceberg-rest-start duckdb-iceberg-rest-recreate 2>/dev/null || true

docker compose build demo
docker compose run --rm demo
