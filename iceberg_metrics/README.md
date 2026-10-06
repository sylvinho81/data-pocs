# Iceberg metrics on MinIO

One JSON document per event, committed to an **Apache Iceberg** table on **MinIO**. A second script reads the commit counters Iceberg stores in table metadata, replays scan planning, and writes both kinds of report into a second Iceberg table.

This follows the counters described in [Iceberg metrics reporting](https://iceberg.apache.org/docs/latest/metrics-reporting/): files added, records added, bytes added, manifests scanned, manifests skipped. The records are shaped like OpenTelemetry log events so each commit or scan is one row you can query.

PyIceberg does not call Java's `MetricsReporter`. The same numbers are still available. Commit counters are written into the snapshot summary. Scan counters are computed again when a scan is planned.

## How a commit is stored

`scripts/generate_seeds.py` writes one JSON object per event to `s3://warehouse/raw-seeds/<run>/<event_id>.json`, then commits those events in random batches of 1 to `SEED_BATCH_MAX` rows (default 4) until none remain.

Each batch is one snapshot on `events.seeds`, partitioned by `city`:

- `added-records` is the batch size.
- `added-data-files` is the number of distinct cities in that batch, because each city becomes its own Parquet file.
- The snapshot summary also stores `commit-attempts`, `commit-started-at-epoch-ms`, `seed.batch-size`, and `seed.event-ids`.

Iceberg then writes a new table metadata JSON under the table location. Every snapshot in that file has a summary map. An append looks like this:

```text
operation: append
added-data-files: 1
added-records: 1
added-files-size: <parquet bytes>
total-data-files: <running total>
total-records: <running total>
total-files-size: <running total>
commit-attempts: 1
commit-started-at-epoch-ms: <unix ms>
```

`added-data-files` is the CommitReport field `addedDataFiles`. The summary uses `deleted-data-files` for what the Java report calls `removedDataFiles`. Totals are the running table size after the commit. Delete counters stay absent on this append-only table, which is the same as a Java CommitReport leaving them null.

`totalDuration` in a Java CommitReport is a timer around the commit. It is not one of Iceberg's built-in summary keys. The seed writer stores the start instant in the summary, and the pull script subtracts it from the snapshot timestamp. `attempts` is `commit-attempts`.

The metadata file path is printed when the writer finishes. It lives on MinIO, next to the Parquet files, for example:

```text
s3a://warehouse/events.db/seeds/metadata/00012-<uuid>.metadata.json
```

Hive lays tables out as `<database>.db/<table>`. The catalog entry in the metastore is only the pointer to that file.

## How a scan is measured

ScanReport numbers are produced while planning a query (how long planning took, how many manifests and files were kept or skipped). Iceberg does not write them into the metadata file.

`scripts/pull_metrics.py` plans two scans against the current snapshot:

- every row
- `city == 'Madrid'`

The table is partitioned by `city`, so the city filter can skip manifests whose partition summary is a different city. Planning time is measured around `plan_files()`. Manifests are split with the same partition-summary check PyIceberg uses.

## The metrics table

The pull script replaces `observability.reports` and appends one row per commit plus one row per scan. That is the OpenTelemetry-style sink: one signal per event, stored where you can query it.

| Column | Commit row | Scan row |
| --- | --- | --- |
| `report_type` | `commit` | `scan` |
| `source` | `snapshot-summary` | `scan-planning` |
| `added_data_files`, `added_records`, `added_files_size_bytes` | from the snapshot summary | empty |
| `duration_ns` | snapshot time minus the stored start instant | planning time |
| `attempts` | `commit-attempts` | empty |
| `result_data_files`, `scanned_data_manifests`, `skipped_data_manifests` | empty | from planning |
| `metrics_json` | `iceberg.commit.*` | `iceberg.scan.*` |
| `attributes_json` | metadata location, manifest list, raw summary | filter and metadata location |

`metrics_json` uses dotted names in the same style as the OpenTelemetry metric names proposed for PyIceberg (`iceberg.commit.data_files.added`, `iceberg.scan.data_manifests.skipped`). `attributes_json` is the resource context: which metadata file, which manifest list, which event.

Writing `observability.reports` is itself an Iceberg commit. Its snapshot summary has `added-data-files=1` and `added-records` equal to the number of report rows. The puller prints that summary and does not write a report about itself.

## Architecture

```text
generate_seeds.py
    JSON object  --->  s3://warehouse/raw-seeds/<run>/<event_id>.json
    one append   --->  events.seeds          (Parquet + snapshot summary on MinIO)
                       Hive metastore        (pointer: metadata_location)

pull_metrics.py
    ask Hive for the pointer
    read snapshot summaries from MinIO  --+
    plan scans                          --+-->  observability.reports
```

Hive is the catalog. It records which metadata file is current for `events.seeds` and `observability.reports`. PyIceberg writes and reads the Parquet, the manifests, and the metadata JSON on MinIO. The Thrift address is `thrift://hive-metastore:9083` inside Compose, and `thrift://localhost:19083` from the host.

Hive names a database folder `<database>.db`. Inside the `warehouse` bucket:

| Folder | What it is |
| --- | --- |
| `raw-seeds/` | One JSON file per event, under `raw-seeds/<run>/<event_id>.json`. These are the original seeds. They are not an Iceberg table and Hive does not track them. |
| `events.db/` | Hive database `events`. The Iceberg table `seeds` lives at `events.db/seeds/`: Parquet files in `data/` (one per city in each batch) and snapshot metadata in `metadata/`. |
| `observability.db/` | Hive database `observability`. The Iceberg table `reports` lives at `observability.db/reports/`: one row per commit or scan, with the same `data/` and `metadata/` layout. |

| Service | URL | Credentials |
| --- | --- | --- |
| MinIO S3 API | http://localhost:19100 | `admin` / `password` |
| MinIO console | http://localhost:19101 | `admin` / `password` |
| Hive metastore | thrift://localhost:19083 | none |

Ports are offset so this stack can run beside the other POCs in this repo.

## Run

```bash
cd iceberg_metrics
docker compose up --build -d
docker compose logs -f seed-writer metrics-puller
```

The seed writer exits after 12 commits. The puller starts when that exit code is 0, prints both reports, and exits. MinIO and the Hive metastore stay up.

To run the scripts on the host against that catalog:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_seeds.py
python scripts/pull_metrics.py
```

`SEED_COUNT` changes how many events (and commits) are written. `SCAN_FILTER` changes the filtered scan. Host defaults talk to `thrift://localhost:19083` and `localhost:19100`.

```bash
docker compose down -v
```
