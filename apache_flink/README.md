# Apache Flink + Kafka + Iceberg — USGS Earthquakes POC

End-to-end **streaming lakehouse** POC: poll the [USGS FDSN Event API](https://earthquake.usgs.gov/fdsnws/event/1/), publish events to **Kafka**, process them continuously with **Apache Flink (PyFlink)**, and land them in **Apache Iceberg** tables on **MinIO** (S3-compatible).

No Spark code is included — Spark is only used as a comparison lens so you can see *why* Flink fits this workload.

---

## Architecture

```text
┌─────────────┐     poll      ┌──────────────┐   JSON events   ┌─────────┐
│ USGS FDSN   │ ────────────► │ Python       │ ───────────────►│  Kafka  │
│ Event API   │  (every 60s)  │ producer     │                 └────┬────┘
└─────────────┘               └──────────────┘                      │
                                                                    │ consume
                                                                    ▼
                                                           ┌────────────────┐
                                                           │ Apache Flink   │
                                                           │ (PyFlink SQL)  │
                                                           │ checkpoints →  │
                                                           │ exactly-once   │
                                                           └───────┬────────┘
                                                                   │ commit
                                                                   ▼
                                                    ┌──────────────────────────┐
                                                    │ Iceberg REST catalog     │
                                                    │ + MinIO (s3://warehouse) │
                                                    │  • earthquakes_raw       │
                                                    │  • earthquakes_by_minute │
                                                    └──────────────────────────┘
```

| Component | Role in this POC |
|-----------|------------------|
| **USGS API** | Source of truth for earthquake events (`format=geojson`) |
| **Python producer** | Incremental poller → Kafka topic `earthquakes` |
| **Kafka** | Durable buffer between ingest and processing |
| **Flink** | Continuous stream processing, watermarks, windowed aggregates, Iceberg writes |
| **Iceberg + MinIO** | ACID table format on object storage (lakehouse) |

---

## Why Flink (vs Spark) for this POC

Spark is excellent for **batch** and large-scale analytics. Spark Structured Streaming is capable, but it is still **micro-batch** oriented. This POC is intentionally a **low-latency, continuous ingest** problem — the kind of workload where Flink’s design shows up clearly.

| Concern | In this POC | Flink | Spark Structured Streaming (typical) |
|---------|-------------|-------|--------------------------------------|
| Processing model | Events arrive continuously from Kafka | Native **event-at-a-time** streaming engine | Micro-batches on a trigger interval |
| Latency to lakehouse | Useful for near-real-time catalogs | Sub-second → few seconds (checkpoint cadence) | Usually seconds → minutes (batch interval) |
| Event time | USGS `time` / revisions can arrive late | First-class **watermarks** (`event_time - 10 minutes`) | Supported, but less natural operationally |
| Stateful windows | `earthquakes_by_minute` mag buckets | Continuous window operator + state backend | Recomputed per micro-batch / stateful store |
| Exactly-once sink | Kafka offsets + Iceberg commits | Checkpoint **2PC** with Iceberg sink | Possible (e.g. foreachBatch + careful design), heavier |
| Job lifetime | Always-on ingest | Long-running streaming job is the default | Often scheduled / triggered streaming jobs |
| Ops mental model | “Is the stream healthy?” | JobManager UI, checkpoints, watermarks | Spark UI batches, trigger offsets |

### What you should notice while running this stack

1. **One long-running job** in the [Flink UI](http://localhost:8081) — not a series of batch runs.
2. **Checkpoints every ~30s** — each successful checkpoint can commit a new Iceberg snapshot (atomic publish of files).
3. **Watermarks** delay closing minute windows until event-time progress is safe — late USGS updates are tolerated instead of being counted in the wrong window.
4. **Append-only Iceberg commits** — each Flink checkpoint publishes a new snapshot. (Iceberg upserts/equality deletes are possible in production; this POC keeps appends so Python readers like PyIceberg can scan easily.)

Spark would still be a strong choice for **reprocessing historical USGS dumps**, heavy SQL analytics, or ML feature batches over the same Iceberg tables. Flink wins here on **continuous ingest + low latency + event-time correctness**.

---

## Project layout

```text
apache_flink/
├── README.md
├── requirements.txt          # Host / producer deps (Kafka client, Iceberg query)
├── requirements-flink.txt    # PyFlink deps for the Flink job image
├── Dockerfile                # Flink 1.18 + PyFlink + Kafka/Iceberg JARs
├── Dockerfile.producer       # USGS → Kafka producer image
├── docker-compose.yml        # Full local stack
├── scripts/
│   ├── submit_job.sh         # Waits for Flink, submits the PyFlink job
│   └── query_iceberg.py      # Read Iceberg tables via REST catalog
└── src/
    ├── producer/
    │   └── usgs_producer.py
    └── flink/
        └── earthquake_job.py
```

---

## Prerequisites

- Docker + Docker Compose v2
- ~4 GB RAM free for the stack
- Optional (host-side query): Python 3.10+ and `pip`

---

## Quick start

```bash
cd apache_flink
docker compose up --build -d
```

First boot builds the Flink image (downloads connector JARs) and may take several minutes.

### Check services

| Service | URL / port | Notes |
|---------|------------|--------|
| Flink UI | http://localhost:18081 | Job `usgs-earthquakes-to-iceberg` should be RUNNING |
| MinIO console | http://localhost:19001 | User `admin` / password `password` |
| MinIO S3 API | http://localhost:19000 | Used by host-side Iceberg clients |
| Iceberg REST | http://localhost:18181 | Catalog API |
| Kafka | `localhost:19092` | From the host |

> Host ports are offset (`19000+`, `18081`, …) to reduce clashes with other local stacks.


```bash
docker compose ps
docker compose logs -f usgs-producer
docker compose logs -f flink-job-submitter
```

### Query Iceberg tables from the host

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/query_iceberg.py
```

Raw rows appear after the producer publishes and Flink completes at least one checkpoint (~30s). Minute aggregates appear after watermarks close windows (can take several minutes of event-time progress).

---

## Configuration

| Variable | Default | Where | Meaning |
|----------|---------|-------|---------|
| `USGS_POLL_SECONDS` | `60` | producer | Poll interval |
| `USGS_MIN_MAGNITUDE` | `2.5` | producer | API `minmagnitude` |
| `USGS_LOOKBACK_HOURS` | `24` | producer | Initial backfill window |
| `KAFKA_TOPIC` | `earthquakes` | producer / Flink | Topic name |
| `CHECKPOINT_INTERVAL_MS` | `30000` | Flink job | Iceberg commit cadence |

---

## Data model

### Kafka / `earthquakes_raw`

Flattened GeoJSON feature fields, including `event_id`, `magnitude`, `place`, `event_time`, coordinates, and `ingested_at`.

### `earthquakes_by_minute`

1-minute **event-time** tumble windows grouped by magnitude bucket (`2.5-2.9` … `6.0+`).

---

## Useful commands

```bash
# Follow producer / Flink
docker compose logs -f usgs-producer flink-jobmanager flink-taskmanager

# Restart only the streaming job submission
docker compose up -d --force-recreate flink-job-submitter

# Tear down (keep MinIO volume)
docker compose down

# Tear down and wipe lakehouse data
docker compose down -v
```

---

## Learning path (if you are new to Flink)

1. Open the Flink UI (http://localhost:18081) → **Running Jobs** → inspect operators (Kafka source → calc → Iceberg sink).
2. Change `CHECKPOINT_INTERVAL_MS` and watch how often Iceberg snapshots advance (`query_iceberg.py` / MinIO `warehouse/` prefixes).
3. Read `src/flink/earthquake_job.py`: watermark clause, `TUMBLE` window, Iceberg catalog DDL.
4. Compare mentally to a Spark job that would `readStream` Kafka and `foreachBatch` write Iceberg every N seconds — same destination, different engine semantics.

---

## API reference

USGS FDSN Event query used by the producer:

`https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minmagnitude=2.5&starttime=...&eventtype=earthquake`

Full docs: https://earthquake.usgs.gov/fdsnws/event/1/

---

## Notes / limitations

- Local POC only: single Kafka broker, single TaskManager, HashMap state backend.
- Windowed aggregates need watermark progress; a quiet period can delay window close (Flink idle/watermark tuning would help in production).
- Small files are expected with frequent checkpoints; production stacks schedule Iceberg compaction.
- USGS asks automated apps to prefer [real-time GeoJSON feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) for display use cases; this POC uses the FDSN query API for flexible historical/incremental pulls.
