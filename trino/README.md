### Trino NYC Taxi POC

This POC builds a small data lake + warehouse stack around Trino on top of the NYC yellow taxi dataset.

### Stack

- **Postgres**: lookup tables (`VendorID`, `RatecodeID`, `payment_type`) and taxi zone lookup.
- **MinIO + [Apache Iceberg](https://iceberg.apache.org/)** (via Trino):
  - **Raw bucket** (`taxi-raw`): original NYC taxi parquet files; exposed in Trino as a Hive external table (`minio_raw.raw.yellow_trip_raw`).
  - **Silver layer**: a real **Apache Iceberg** table in MinIO (`minio_iceberg.taxi.yellow_trip_silver`). Trino creates Iceberg metadata (metadata.json, manifests) and Parquet data files when you run `sql/01_setup_iceberg_silver.sql` (CREATE TABLE + INSERT from raw).
- **ClickHouse**: star schema with a `fact_yellow_trip` fact table and dimension tables copied from Postgres.
- **Trino**: federated query engine with catalogs for Postgres, MinIO (raw Hive + Iceberg), and ClickHouse.

### How Trino catalogs and federation work

Trino exposes every backend system (Postgres, Iceberg on MinIO, ClickHouse, …) as a **catalog**. Each catalog is configured by a small
`*.properties` file under `conf/trino/catalog/` that tells Trino which connector to use and how to connect:

- **`postgres` catalog** (configured in `postgres.properties`): uses the Trino Postgres connector to talk to the Postgres container.
- **`minio_raw` catalog**: uses the Hive connector, pointing at the Hive metastore (backed by MariaDB) and the `taxi-raw` MinIO bucket.
- **`minio_iceberg` catalog**: uses the Iceberg connector, pointing at the same Hive metastore and the `trino-poc-iceberg` MinIO bucket.
- **`clickhouse` catalog**: uses the ClickHouse connector, pointing at the ClickHouse container and the `taxi` database.

In SQL, a fully qualified table name is:

```text
<catalog>.<schema>.<table>
```

For example:

- `postgres.public.dim_vendor` – lookup table in Postgres.
- `minio_iceberg.taxi.yellow_trip_silver` – Iceberg table stored in MinIO.
- `clickhouse.taxi.fact_yellow_trip` – fact table in ClickHouse.

Because Trino sees all of these as just tables in different catalogs, you can **join across systems in a single query**. The
`sql/analytics_iceberg_postgres.sql` file shows queries like:

- fact data in `minio_iceberg.taxi.yellow_trip_silver` joined to lookup tables in `postgres.public.dim_*`.

and `sql/analytics_clickhouse_star.sql` shows:

- fact data and dimensions both in `clickhouse.taxi.*` (classic star schema in a single engine).

You do not have to configure anything special to "enable" joins between Iceberg and Postgres: as long as both catalogs exist and
you reference the fully qualified names, Trino’s planner will push as much work as possible down to each engine and do the rest
of the join work itself.

### Layout

- `docker-compose.yml`: services (Postgres, MinIO, **MariaDB** for Hive metastore, Hive Metastore, ClickHouse, Trino). Same pattern as [trino_delta_lake](../trino_delta_lake/).
- `conf/metastore-site.xml`, `conf/core-site.xml`: Hive metastore config (MariaDB + S3/MinIO).
- `conf/trino/*`: Trino config and catalogs (`postgres`, `minio_iceberg`, `minio_raw`, `clickhouse`).
- `src/settings.py`: paths and connection settings.
- `src/etl_postgres.py`: builds Postgres schema and lookup tables.
- `src/etl_minio_iceberg.py`: uploads raw parquet to MinIO only; silver is created as Iceberg in Trino.
- `src/etl_clickhouse.py`: builds ClickHouse star schema and loads data.
- `sql/01_setup_iceberg_silver.sql`: creates Hive raw table + Apache Iceberg silver table and loads silver from raw.
- `sql/analytics_iceberg_postgres.sql`: Trino queries over Iceberg + Postgres.
- `sql/analytics_clickhouse_star.sql`: Trino queries over the ClickHouse star schema.

### Prerequisites

- Docker & Docker Compose.
- Python 3.10+.

### 1. Start the stack

From `trino/`:

```bash
docker compose up -d
```

Wait until all containers are running. A **wait-for-metastore** step ensures Trino starts only after the Hive metastore is listening on 9083 (MariaDB is used for metastore storage, as in [trino_delta_lake](../trino_delta_lake/)). If you still see "Trino server is still initializing" or "Failed connecting to Hive metastore", wait 1–2 minutes and retry.

### 2. Install Python dependencies

From `trino/src`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Load lookup data into Postgres

From `trino/src`:

```bash
python etl_postgres.py
```

This creates:

- `dim.vendor`, `dim.rate_code`, `dim.payment_type`.
- `dim.taxi_zone` populated from `raw_files/taxi_zone_lookup.csv`.

### 3.5. Prepare local parquet files

Before staging raw data in MinIO, make sure the local parquet folder exists and contains the NYC **yellow taxi trip record** parquet files:

- **Data source**: NYC TLC Trip Record Data – see the **Trip Record Data Download Links** section on the official page  
  (`https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`).
- For each year/month you want to include, download the **"Yellow Taxi Trip Records"** files (they are already in PARQUET format).
- Save the downloaded parquet files under:
  - **Project-relative path**: `parquet_files/ny_taxi_raw`

The script `src/etl_minio_iceberg.py` reads those yellow taxi parquet files from this folder and uploads them into the MinIO raw bucket `taxi-raw/ny_taxi_raw/`.

### 4. Stage raw data in MinIO and create the Apache Iceberg silver table

From `trino/src`:

```bash
python etl_minio_iceberg.py
```

This uploads all `parquet_files/ny_taxi_files/*.parquet` files into the **raw** MinIO bucket (`taxi-raw/ny_taxi_raw/`).

Then run the setup script so the **silver** layer is a real [Apache Iceberg](https://iceberg.apache.org/) table (metadata + Parquet data written by Trino). Use one of these methods:

**Option A – Trino Web UI (easiest)**

1. Open **http://localhost:8080** in your browser. No authentication is configured—use any username (e.g. `trino`) or leave it blank if the UI allows.
2. **Where to type the query:** In the top navigation bar, click **"Query"** (or **"Execute"**). You should see a large **query editor** (text area) in the main part of the page—type or paste your SQL there. Use the **Run** button (or **Ctrl+Enter** / **Cmd+Enter**) to execute. If you land on the overview/dashboard, the query box is usually one click away under the "Query" menu.
3. Open `sql/01_setup_iceberg_silver.sql` in an editor.
3. Run the statements **in order**, one block at a time (or all at once if the UI allows):
   - First block: `CREATE SCHEMA` and `CREATE TABLE` for `minio_raw.raw.yellow_trip_raw`.
   - Second block: `CREATE SCHEMA` and `CREATE TABLE` for `minio_iceberg.taxi.yellow_trip_silver`.
   - Third block: the `INSERT INTO ... SELECT ... FROM minio_raw.raw.yellow_trip_raw` (this may take a few minutes).

**Option B – Trino CLI via Docker (no install)**

Run the Trino CLI inside a container (uses the same image as the server). **From the `trino/` directory**, ensure the stack is up first (`docker compose up -d`). Use the server’s **trino-server** in the commands below; a hostname with underscores causes `UnknownHostException: null` for all queries.

- **Interactive:** open a CLI session and type SQL, then exit with `quit` or Ctrl+D.
  ```bash
  docker compose run --rm trino trino --server http://trino-server:8080
  ```
- **Run a SQL file:**
  ```bash
  docker compose run --rm -v "$(pwd)/sql:/sql" trino trino --server http://trino-server:8080 -f /sql/01_setup_iceberg_silver.sql
  ```

**Option C – Trino CLI installed on your machine**

If you install the [Trino CLI](https://trino.io/docs/current/installation.html#trino-cli) on your host, you can run:

```bash
cd /data-pocs/trino
trino --server http://localhost:8080
# or run a file:
trino --server http://localhost:8080 -f sql/01_setup_iceberg_silver.sql
```

**What the script does**

1. Creates a Hive external table `minio_raw.raw.yellow_trip_raw` over the raw parquet path in MinIO.
2. Creates the Iceberg schema and table `minio_iceberg.taxi.yellow_trip_silver`.
3. Runs `INSERT INTO ... SELECT` from raw into silver so Trino writes Iceberg metadata and Parquet data files into MinIO.

After this, `yellow_trip_silver` is a full Iceberg table (time travel, schema evolution, etc.).

**If it doesn’t work**

- **"Trino server is still initializing"**: Wait 1–2 minutes after `docker compose up -d`, then run your query again. Check the Trino container logs: `docker compose logs -f trino` until you see the server fully started.
- **Trino not reachable**: Ensure the stack is up (`docker compose ps`) and Trino is healthy; wait a minute after `up -d` for Trino to finish starting.
- **Catalog not found** (`minio_raw` or `minio_iceberg`): Check `conf/trino/catalog/` has `minio_raw.properties` and `minio_iceberg.properties` and that Trino was restarted after adding them.
- **"Failed connecting to Hive metastore"**: The Hive metastore can take 1–2 minutes to open port 9083 after the container starts. After `docker compose up -d`, wait 2 minutes before running the Iceberg setup SQL. If it still fails, run `docker compose restart trino` and retry the query.
- **Raw table empty or INSERT fails**: Ensure `python etl_minio_iceberg.py` completed and the MinIO bucket `taxi-raw` has objects under `ny_taxi_raw/` (e.g. in MinIO Console at http://localhost:9001).
- Run the SQL **one statement at a time** in the Web UI to see which command fails and use the error message to fix config or data.

### 5. Build the ClickHouse star schema

From `trino/src`:

```bash
python etl_clickhouse.py
```

This:

- Creates ClickHouse database `taxi`.
- Creates `dim_vendor`, `dim_rate_code`, `dim_payment_type`, `dim_taxi_zone`.
- Copies the dimension data from Postgres into ClickHouse.
- Creates `fact_yellow_trip` and loads up to 1M rows from the raw parquet files (processed file-by-file and inserted in 100k-row batches to avoid OOM).

**If the process is "Killed"** (e.g. by the OS OOM killer), reduce memory use by lowering the row limit or batch size in code (`load_fact_from_parquet(limit_rows=200_000)` or a smaller `insert_batch_size`), or run on a machine with more RAM.

### 6. Run analytics in Trino

You can now run analytics in Trino using two approaches.

- **Iceberg + Postgres** (federated lookups):
  - Open `sql/analytics_iceberg_postgres.sql` and run the queries in Trino.

- **ClickHouse star schema**:
  - Open `sql/analytics_clickhouse_star.sql` and run the queries in Trino.

Both sets of queries compute daily revenue, revenue by zone/borough, and payment mix, demonstrating:

- Federation between Iceberg data in MinIO and lookup tables in Postgres.
- Star-schema analytics pushed down into ClickHouse.

