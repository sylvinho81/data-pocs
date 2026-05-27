# dbt POC — NYC taxi analytics with Polars + ClickHouse

This proof-of-concept loads NYC yellow taxi parquet data into ClickHouse with **Polars**, then builds the same summary statistics as [`benchmark_vortex_parquet/benchmark.py`](../benchmark_vortex_parquet/benchmark.py) using **dbt** models you can query directly in ClickHouse.

## Architecture

```text
yellow_tripdata_2025-11.parquet
            │
            ▼
   scripts/load_parquet.py   (Polars read + ClickHouse insert)
            │
            ├── nyc_taxi.yellow_trip              (raw fact table)
            └── seeds/random_sample_location_ids  (50 fixed location IDs)
            │
            ▼
         dbt run
            │
            └── nyc_taxi.* analytics (7 tables + 1 view)
```

## DBT HOW IT WORKS

### What is dbt, and why use it here?

**dbt** (data build tool) is a framework for turning raw data in a warehouse into clean, documented, queryable tables using SQL. You write `SELECT` statements; dbt handles the plumbing — dependency order, table creation, and re-runs when something changes.

In this POC, the same statistics could be computed entirely in Python with Polars (as in `benchmark.py`). dbt is introduced deliberately to separate concerns:

| Layer | Tool | Responsibility |
|-------|------|----------------|
| **Ingest** | Polars + `load_parquet.py` | Read parquet from disk, load into ClickHouse |
| **Transform** | dbt | Define analytics as version-controlled SQL models |
| **Query** | ClickHouse | Serve the final tables to any client |

That split mirrors how many teams work in production: engineers or pipelines load data; analytics engineers define transformations in dbt; the warehouse runs the queries.

**What you gain by using dbt in this POC:**

- **SQL as the transformation language** — analytics are plain ClickHouse SQL, easy to read and tweak without touching Python.
- **Dependency graph** — dbt knows that `random_access_location_filter` depends on the seed table and runs models in the right order.
- **Reproducibility** — `dbt run` rebuilds all eight analytics models from the same source data, the same way, every time.
- **A path to grow** — you can add tests, documentation, incremental models, and more models without rewriting the ingest script.

### End-to-end flow in this POC

```text
1. INGEST (outside dbt)
   load_parquet.py
   ├── writes nyc_taxi.yellow_trip          ← raw trips in ClickHouse
   └── writes seeds/random_sample_location_ids.csv

2. DBT SEED
   dbt seed
   └── loads CSV → nyc_taxi.random_sample_location_ids

3. DBT RUN
   dbt run
   └── reads yellow_trip (+ seed where needed)
       └── creates 7 analytics tables + 1 view in nyc_taxi
```

Polars owns step 1. dbt owns steps 2 and 3. ClickHouse stores everything.

### What happens when you run dbt

**`dbt seed`** reads CSV files from `nyc_taxi/seeds/` and loads them into ClickHouse as tables. Here, the only seed is `random_sample_location_ids` — 50 pickup location IDs sampled from the parquet file (with a fixed random seed so results are reproducible). That seed powers the "random access" benchmark analysis.

**`dbt run`** compiles each `.sql` file under `models/analytics/`, resolves Jinja placeholders like `{{ source(...) }}` and `{{ ref(...) }}` into real table names, and executes the SQL against ClickHouse. Because models are configured with `+materialized: table`, dbt creates (or replaces) a physical table for each analysis.

### Configuration YAML files

dbt is configured through YAML files rather than code. This POC uses three of them. Together they answer: *what is this project*, *how does dbt connect to ClickHouse*, and *which external tables can models read from*.

#### `dbt_project.yml` — project definition

This file lives at the root of the dbt project (`nyc_taxi/`). dbt reads it on every command to know where models, seeds, and macros live, and to apply default settings to all resources.

```yaml
name: nyc_taxi
version: 1.0.0
config-version: 2

profile: nyc_taxi

model-paths: ["models"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - target
  - dbt_packages

models:
  nyc_taxi:
    analytics:
      +materialized: table

seeds:
  nyc_taxi:
    random_sample_location_ids:
      +column_types:
        location_id: Int32
```

| Field | Purpose in this POC |
|-------|---------------------|
| `name: nyc_taxi` | Project identifier. Must match the top-level key under `models:` and `seeds:` when scoping config. |
| `version: 1.0.0` | Semantic version of the project (informational). |
| `config-version: 2` | dbt config schema version. Always `2` for modern dbt projects. |
| `profile: nyc_taxi` | Links this project to a profile name in `profiles.yml`. dbt uses that profile to connect to ClickHouse. |
| `model-paths` | Directory(ies) where `.sql` model files live. All analytics models are under `models/analytics/`. |
| `seed-paths` | Directory where CSV seed files live (`seeds/random_sample_location_ids.csv`). |
| `macro-paths` | Directory for reusable Jinja macros. Empty in this POC (`macros/` does not exist yet), but dbt expects the path to be declared. |
| `clean-targets` | Folders removed by `dbt clean` — compiled artifacts (`target/`) and installed packages (`dbt_packages/`). |
| `models.nyc_taxi.analytics.+materialized: table` | Applies to every model in `models/analytics/`. The `+` prefix means "merge with any model-level config." `table` tells dbt to persist each model as a physical ClickHouse table (not a view). |
| `seeds.nyc_taxi.random_sample_location_ids.+column_types` | Overrides the inferred type for the `location_id` column when loading the seed CSV. Set to `Int32` to match ClickHouse and the `pu_location_id` column in `yellow_trip`. |

#### How `model-paths` relates to the `models:` config block

These are **two different things** that people often mix up:

| Concept | What it is | In this POC |
|---------|------------|-------------|
| **`model-paths`** | A **filesystem** setting — "where on disk should dbt look for `.sql` files?" | `["models"]` → dbt scans `nyc_taxi/models/` and all subfolders recursively |
| **`models:` in YAML** | A **configuration** tree — "what defaults should apply to which models?" | Not a path on disk |

On disk, the project looks like this:

```text
nyc_taxi/
├── dbt_project.yml
├── models/
│   ├── _sources.yml
│   └── analytics/                    ← subfolder under model-paths
│       ├── payment_types.sql         ← model name = "payment_types"
│       ├── airport_fee.sql
│       └── ...
└── seeds/
    └── random_sample_location_ids.csv
```

There is **no** `models/nyc_taxi/` folder. The SQL files live directly under `models/analytics/`.

So why does the YAML say `models → nyc_taxi → analytics`?

```yaml
models:          # ① dbt keyword: "here comes model configuration"
  nyc_taxi:      # ② project name (must match `name: nyc_taxi` at top of file)
    analytics:   # ③ subfolder name under model-paths (matches models/analytics/)
      +materialized: table
```

- **① `models:`** — top-level key in every `dbt_project.yml` for model settings (materialization, tags, etc.). It has nothing to do with the `models/` directory name — unfortunate naming overlap.
- **② `nyc_taxi:`** — the **project scope**. dbt requires the project `name` as the first level under `models:` so config can be namespaced when you have multiple projects or packages. It is **not** a folder.
- **③ `analytics:`** — the **subfolder** inside `model-paths`. Because files are in `models/analytics/*.sql`, config under `analytics:` applies to all of them.

Mapping in one picture:

```text
FILESYSTEM                          dbt_project.yml CONFIG
──────────                          ────────────────────────

models/                        →    (model-paths — not repeated in models: block)
  analytics/                   →    models:
    payment_types.sql          →      nyc_taxi:          ← project name, NOT a folder
                                      analytics:         ← matches "analytics/" folder
                                        +materialized: table
```

**Model name vs folder:** dbt names each model from the **filename**, not the folder. `models/analytics/payment_types.sql` becomes model `payment_types`, stored in ClickHouse as table `nyc_taxi.payment_types`. The `analytics/` folder only groups files and lets you attach config to that group.

**What if you added `models/staging/`?** You would add a sibling block:

```yaml
models:
  nyc_taxi:
    staging:
      +materialized: view
    analytics:
      +materialized: table
```

All files in `models/staging/` would get `view`; all files in `models/analytics/` would get `table`.

**Seeds follow the same pattern:** `seeds → nyc_taxi → random_sample_location_ids` means "config for the seed file `seeds/random_sample_location_ids.csv`". The middle level is again the project name; the last level is the **filename without `.csv`**, not a subfolder.

#### `profiles.yml` — warehouse connection

This file tells dbt **where** ClickHouse is and **which database** to use. It is not part of the transformation logic; it is pure connection metadata.

```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: nyc_taxi
      host: localhost
      port: 8123
      user: default
      password: ""
      secure: false
      verify: false
```

| Field | Purpose in this POC |
|-------|---------------------|
| `nyc_taxi` (top-level key) | Profile name. Must match `profile:` in `dbt_project.yml`. |
| `target: dev` | Active environment. dbt uses the `dev` output block below. You could add `prod` with different host/credentials later. |
| `type: clickhouse` | Selects the `dbt-clickhouse` adapter. dbt translates generic commands (`dbt run`, `dbt seed`) into ClickHouse-specific SQL. |
| `schema: nyc_taxi` | **ClickHouse database name.** In ClickHouse, a "database" is what dbt calls a "schema". All models, seeds, and sources in this POC live in `nyc_taxi`. |
| `host` / `port` | ClickHouse HTTP interface. Matches `docker-compose.yml` (`8123`). Used by `dbt-clickhouse` to send queries. |
| `user` / `password` | ClickHouse credentials. The default Docker image uses user `default` with no password. |
| `secure: false` | No TLS/HTTPS for local development. |
| `verify: false` | Skip SSL certificate verification (only relevant if `secure` were `true`). |

By default dbt looks for `profiles.yml` in `~/.dbt/`. This POC keeps it inside the project at `nyc_taxi/profiles.yml` and sets:

```bash
export DBT_PROFILES_DIR="$(pwd)/nyc_taxi"
```

so the connection settings travel with the repo.

#### `models/_sources.yml` — external tables dbt reads but does not build

Sources document tables that **exist before dbt runs**. They are loaded by another process — here, `load_parquet.py` — and referenced in model SQL via `{{ source('raw', 'yellow_trip') }}`.

```yaml
version: 2

sources:
  - name: raw
    description: Raw yellow taxi trips loaded from parquet via Polars
    schema: nyc_taxi
    tables:
      - name: yellow_trip
        description: November 2025 yellow taxi trip records
```

| Field | Purpose in this POC |
|-------|---------------------|
| `version: 2` | Sources schema version for dbt. Required. |
| `sources` | List of source groups. Each group is a logical namespace for related tables. |
| `name: raw` | Source group name. First argument to `source()`: `source('raw', 'yellow_trip')`. Chosen to signal "raw layer" data, not yet transformed by dbt. |
| `description` | Human-readable docs. Shown in `dbt docs generate` if you enable documentation later. |
| `schema: nyc_taxi` | ClickHouse database where the table lives. Must match the database created by `load_parquet.py`. |
| `tables` | List of tables within this source group. |
| `name: yellow_trip` | Table name in ClickHouse. Second argument to `source()`. Created by Polars ingest, not by dbt. |
| `description` (on table) | Documents what the table contains (November 2025 yellow taxi trips). |

When dbt compiles a model like `payment_types.sql`, it replaces:

```sql
{{ source('raw', 'yellow_trip') }}
```

with the fully qualified name:

```sql
nyc_taxi.yellow_trip
```

Sources also let dbt track lineage: dbt knows analytics models **depend on** `yellow_trip` even though dbt never creates that table. You can add source freshness checks or tests on sources in a larger project; this POC keeps it minimal.

#### How the three files work together

```text
dbt_project.yml          profiles.yml              _sources.yml
     │                        │                          │
     │  profile: nyc_taxi ────►│  connect to ClickHouse   │
     │                        │  database: nyc_taxi      │
     │                        │                          │
     │  models/analytics/     │                          │
     │  +materialized: table ─┼──► CREATE TABLE in       │
     │                        │    nyc_taxi.*            │
     │                        │                          │
     │  model SQL uses ───────┼──────────────────────────► source('raw', 'yellow_trip')
     │                        │                          │         │
     │                        │                          │         ▼
     │                        │                    nyc_taxi.yellow_trip
     │                        │                    (loaded by Polars)
```

- **`dbt_project.yml`** — *what* to build and *how* (paths, materializations, seed types).
- **`profiles.yml`** — *where* to build it (ClickHouse host, database, credentials).
- **`_sources.yml`** — *which existing tables* models are allowed to read from.

### Key dbt concepts used in this project

**Sources** — tables loaded outside dbt that dbt reads from but does not create. Declared in `models/_sources.yml`; see [Configuration YAML files](#configuration-yaml-files) for field-by-field detail.

`yellow_trip` is referenced in models with:

```sql
{{ source('raw', 'yellow_trip') }}
```

The load script is responsible for keeping it populated; dbt only reads it.

**Seeds** — small, static reference datasets checked into the repo as CSV. Column types for `random_sample_location_ids` are set in `dbt_project.yml`.

The random location IDs are a seed because they are derived once from the data but then treated as fixed input for a specific analysis — similar to a lookup table.

**Refs** — references between dbt models (or seeds).

`random_access_location_filter.sql` uses:

```sql
{{ ref('random_sample_location_ids') }}
```

dbt builds a dependency graph from these refs and runs `random_sample_location_ids` (via seed) before any model that references it.

**Models** — one `.sql` file per analytics output.

Each file in `models/analytics/` is a model. For example, `payment_types.sql` groups trips by payment type and sums revenue. dbt turns that SELECT into a ClickHouse table named `payment_types`. Default materialization (`table`) comes from `dbt_project.yml`.

**Materialization** — how dbt persists a model.

All analytics models default to `table` materialization (configured in `dbt_project.yml`): dbt runs `CREATE TABLE … AS SELECT …` via the ClickHouse adapter so results are stored and queryable without re-running the aggregation every time.

One model overrides this at the file level: `trips_by_hour_of_day.sql` uses `{{ config(materialized='view') }}`, so ClickHouse stores a view that re-runs the aggregation on each query instead of persisting rows.

**Profiles** — connection settings for the warehouse.

Defined in `profiles.yml`; see [Configuration YAML files](#configuration-yaml-files). Point dbt at it with `DBT_PROFILES_DIR`.

### Model dependency graph

```text
yellow_trip (source, loaded by Polars)
    │
    ├── trips_by_day_of_week
    ├── trips_by_hour_of_day          (view)
    ├── payment_types
    ├── passenger_count
    ├── rides_by_month
    ├── airport_fee
    ├── rides_by_location
    │
    └── random_access_location_filter
            ▲
            │
random_sample_location_ids (seed)
```

Six models read only from `yellow_trip`. One model joins the fact table to the seed. `trips_by_hour_of_day` is a view over the same source.

### What dbt is not doing here

- dbt does **not** read the parquet file — that is Polars' job.
- dbt does **not** replace ClickHouse — it sends SQL to ClickHouse and materializes results there.
- dbt does **not** run a scheduler — you invoke `dbt seed` and `dbt run` manually (or wire them into a script/CI later).

The point of the POC is to experience that boundary: ingest once, transform declaratively in SQL, query the warehouse.

### Design decisions

| Choice | Rationale |
|--------|-----------|
| **Polars for ingest, dbt for analytics** | Polars handles parquet I/O efficiently; dbt owns the transformation layer so you learn dbt concepts (sources, refs, seeds, materializations) without mixing SQL into Python. |
| **ClickHouse as the warehouse** | Single analytical database for this POC — fast aggregations and easy querying once dbt materializes the models. |
| **Snake_case in raw table** | ClickHouse column names are normalized on load (`pu_location_id`, `airport_fee`) to avoid quoting issues in dbt SQL. |
| **Fixed seed for random access** | The benchmark samples 50 random `PULocationID` values. The load script writes them to a dbt seed with `random.seed(42)` so results are reproducible across runs. |
| **Table materialization** | Analytics models are materialized as ClickHouse tables so you can query them with any client (HTTP, `clickhouse-client`, Grafana, etc.). |

## Analyses (mirrors benchmark.py)

| dbt model | Benchmark analysis |
|-----------|-------------------|
| `trips_by_day_of_week` | Trips by day of week |
| `payment_types` | Payment types and revenue |
| `passenger_count` | Passenger count distribution |
| `rides_by_month` | Monthly rides + congestion fee |
| `airport_fee` | Airport fee percentage |
| `rides_by_location` | Top 20 pickup locations |
| `random_access_location_filter` | Count trips for 50 sampled location IDs |
| `trips_by_hour_of_day` | Trips by hour of day (**view**, not a table) |

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- The parquet file at `files/yellow_tripdata_2025-11.parquet` (included in this repo)

## STEPS TO RUN IT

All commands below assume you start from the repository root and then enter the `dbt/` directory.

### Step 1 — Go to the project directory

```bash
cd dbt
```

### Step 2 — Create and activate a Python virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

This installs **Polars** and **PyArrow** (parquet ingest and Arrow conversion), **clickhouse-connect** (loading data into ClickHouse), and **dbt-core** + **dbt-clickhouse** (transformation layer).

### Step 4 — Start ClickHouse with Docker Compose

```bash
docker compose up -d
```

This starts a ClickHouse 24.1 container named `dbt_poc_clickhouse` and exposes:

- HTTP API on port **8123** (used by dbt and `clickhouse-connect`)
- Native protocol on port **9000**

### Step 5 — Wait until ClickHouse is ready

```bash
curl http://localhost:8123/ping
```

Expected output: `Ok.`

If the command fails, wait a few seconds and retry. ClickHouse may take 10–20 seconds to become available on first start.

You can also check container status:

```bash
docker compose ps
```

### Step 6 — Load the parquet file into ClickHouse

```bash
python scripts/load_parquet.py --recreate
```

This script:

1. Reads `files/yellow_tripdata_2025-11.parquet` with Polars (~4.1M rows).
2. Creates the `nyc_taxi` database and `yellow_trip` table in ClickHouse (drops the table first because of `--recreate`).
3. Inserts the data in batches.
4. Writes 50 sampled pickup location IDs to `nyc_taxi/seeds/random_sample_location_ids.csv` (fixed random seed for reproducibility).

Expected output ends with something like:

```text
Loaded 4,181,444 rows.
```

### Step 7 — Point dbt to the local profiles file

dbt reads connection settings from `nyc_taxi/profiles.yml`. Tell dbt to use that directory:

```bash
export DBT_PROFILES_DIR="$(pwd)/nyc_taxi"
```

On Windows (PowerShell):

```powershell
$env:DBT_PROFILES_DIR = "$(pwd)/nyc_taxi"
```

You need this variable set in every new shell session before running dbt commands.

### Step 8 — Enter the dbt project directory

```bash
cd nyc_taxi
```

### Step 9 — Load dbt seeds

```bash
dbt seed
```

This loads `seeds/random_sample_location_ids.csv` into ClickHouse as the table `random_sample_location_ids`. That table is used by the random-access analysis model.

### Step 10 — Run dbt models

```bash
dbt run
```

dbt builds eight analytics models in the `nyc_taxi` database (seven tables and one view):

- `trips_by_day_of_week`
- `trips_by_hour_of_day` (view)
- `payment_types`
- `passenger_count`
- `rides_by_month`
- `airport_fee`
- `rides_by_location`
- `random_access_location_filter`

A successful run ends with `Completed successfully` and all eight models marked `OK`.

### Step 11 — Verify the results

Pick any analytics table and query it. Examples:

**HTTP:**

```bash
curl 'http://localhost:8123/?query=SELECT+*+FROM+nyc_taxi.payment_types+ORDER+BY+payment_type+FORMAT+PrettyCompact'
```

**clickhouse-client inside the container:**

```bash
docker exec -it dbt_poc_clickhouse clickhouse-client \
  --query "SELECT * FROM nyc_taxi.rides_by_location"
```

**Python:**

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host="localhost", port=8123)
print(client.query("SELECT * FROM nyc_taxi.airport_fee").result_rows)
```

**Example — query the `trips_by_hour_of_day` view:**

This model is materialized as a ClickHouse **view** (not a table). You query it the same way, but ClickHouse runs the underlying aggregation on each request instead of reading stored rows.

```sql
SELECT *
FROM nyc_taxi.trips_by_hour_of_day
ORDER BY hour_of_day;
```

HTTP:

```bash
curl 'http://localhost:8123/?query=SELECT+*+FROM+nyc_taxi.trips_by_hour_of_day+ORDER+BY+hour_of_day+FORMAT+PrettyCompact'
```

clickhouse-client:

```bash
docker exec -it dbt_poc_clickhouse clickhouse-client \
  --query "SELECT * FROM nyc_taxi.trips_by_hour_of_day ORDER BY hour_of_day"
```

To confirm it is a view and not a table:

```bash
docker exec -it dbt_poc_clickhouse clickhouse-client \
  --query "SELECT name, engine FROM system.tables WHERE database = 'nyc_taxi' AND name = 'trips_by_hour_of_day'"
```

Expected `engine` value: `View`.

### Optional — Run everything in one script

If you prefer a single entry point after Steps 2–3:

```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh
```

The script runs Steps 4–10 automatically (start ClickHouse, wait for it, load parquet, `dbt seed`, `dbt run`). You still need the virtualenv active and dependencies installed first.

### Re-running from scratch

To wipe ClickHouse data and rebuild:

```bash
docker compose down -v
docker compose up -d
curl http://localhost:8123/ping
python scripts/load_parquet.py --recreate
export DBT_PROFILES_DIR="$(pwd)/nyc_taxi"
cd nyc_taxi
dbt seed
dbt run
```

## Query the results

See Step 11 above for HTTP, `clickhouse-client`, and Python examples.

All analytics tables live in the `nyc_taxi` database and can be queried with standard SQL.

## Project layout

```text
dbt/
├── docker-compose.yml          # ClickHouse 24.1
├── requirements.txt            # polars, pyarrow, clickhouse-connect, dbt-clickhouse
├── files/
│   └── yellow_tripdata_2025-11.parquet
├── scripts/
│   ├── load_parquet.py         # Polars → ClickHouse ingest
│   └── run_pipeline.sh         # One-shot setup + dbt run
└── nyc_taxi/                   # dbt project
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── _sources.yml
    │   └── analytics/          # 8 models (7 tables + 1 view)
    └── seeds/
        └── random_sample_location_ids.csv
```

## dbt concepts (quick reference)

See [DBT HOW IT WORKS](#dbt-how-it-works) for the full explanation. In short: **sources** for raw data loaded outside dbt, **seeds** for the location ID CSV, **refs** for model-to-model dependencies, **table materializations** for persisted analytics, and **profiles** for the ClickHouse connection.

## Extending the POC

- Add more parquet files: extend `load_parquet.py` to glob `files/*.parquet`.
- Add dbt tests: `unique`, `not_null` on keys, or custom SQL tests comparing to Polars baselines.
- Add incremental models if you load data in batches over time.
- Join a taxi zone lookup table as another seed or source for richer location names.

## Cleanup

```bash
docker compose down -v   # removes ClickHouse data volume
```
