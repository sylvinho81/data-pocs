#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER="${FLINK_JOBMANAGER:-flink-jobmanager:8081}"
JOB_FILE="${JOB_FILE:-/opt/flink/usrlib/earthquake_job.py}"

echo "Waiting for Flink JobManager at http://${JOBMANAGER} ..."
for i in $(seq 1 90); do
  if curl -sf "http://${JOBMANAGER}/overview" >/dev/null; then
    echo "Flink is up."
    break
  fi
  if [[ "$i" -eq 90 ]]; then
    echo "Timed out waiting for Flink JobManager" >&2
    exit 1
  fi
  sleep 2
done

# Give the TaskManager a moment to register
sleep 5

if curl -sf "http://${JOBMANAGER}/jobs/overview" \
  | grep -qiE '"name":"usgs-earthquakes-to-iceberg"[^}]*"state":"RUNNING"'; then
  echo "Earthquake job already RUNNING — skipping submit."
  exit 0
fi

echo "Submitting PyFlink job ${JOB_FILE} ..."
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-password}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

flink run \
  -d \
  -m "${JOBMANAGER}" \
  -py "${JOB_FILE}" \
  -Dpipeline.name=usgs-earthquakes-to-iceberg

echo "Job submitted. Open the Flink UI at http://localhost:18081"
sleep 8
curl -sf "http://${JOBMANAGER}/jobs/overview" || true
echo
