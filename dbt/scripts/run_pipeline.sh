#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Starting ClickHouse"
docker compose up -d

echo "==> Waiting for ClickHouse HTTP port"
for i in $(seq 1 30); do
  if curl -sf "http://localhost:8123/ping" >/dev/null; then
    echo "ClickHouse is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Timed out waiting for ClickHouse." >&2
    exit 1
  fi
  sleep 2
done

echo "==> Loading parquet with Polars"
python scripts/load_parquet.py "$@"

echo "==> Running dbt"
export DBT_PROFILES_DIR="$ROOT/nyc_taxi"
cd nyc_taxi
dbt seed
dbt run

echo "==> Done. Example queries:"
echo "  curl 'http://localhost:8123/?query=SELECT+*+FROM+nyc_taxi.trips_by_day_of_week+FORMAT+PrettyCompact'"
