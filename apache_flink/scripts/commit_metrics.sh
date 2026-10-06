#!/usr/bin/env bash
# Print Iceberg CommitReport metrics inside the compose network.
# Extra args are forwarded, for example: ./scripts/commit_metrics.sh --bucket hour --last 0
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

exec docker compose --profile metrics run --rm commit-metrics "$@"
