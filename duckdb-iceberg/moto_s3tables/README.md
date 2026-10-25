# Moto + S3 Tables (sidecar experiment)

Standalone experiment under `duckdb-iceberg/` that uses **[moto](https://github.com/getmoto/moto)** to mock **Amazon S3 Tables** APIs locally. It does **not** replace the MinIO + Iceberg REST POC in the parent folder.

## What works with moto

Moto implements the S3 Tables **control plane** (boto3 client `s3tables`):

- `create_table_bucket` / `list_table_buckets` / `get_table_bucket`
- `create_namespace` / `list_namespaces`
- `create_table` / `list_tables` / `get_table` / `rename_table` / …
- `update_table_metadata_location` (partial; see coverage)

Coverage: [moto s3tables docs](https://docs.getmoto.org/en/stable/docs/services/s3tables.html).

## What does **not** work yet (DuckDB path)

Parent POC Option A uses:

```sql
ATTACH 'arn:aws:s3tables:...' AS lake (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);
```

DuckDB then calls the AWS **Iceberg REST** endpoint:

`https://s3tables.<region>.amazonaws.com/iceberg`

That facade translates Iceberg REST → S3 Tables (see [AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)). **Moto does not implement `/iceberg/v1/...`**, so pointing DuckDB at `moto_server` returns 404, and `ENDPOINT_TYPE s3_tables` always hits real AWS.

| Layer | MinIO + REST (parent) | Moto s3tables (this folder) | Real S3 Tables |
|-------|------------------------|-----------------------------|----------------|
| boto3 `s3tables` catalog ops | N/A | Yes | Yes |
| DuckDB `MERGE` / Iceberg writes | Yes (REST catalog) | No (no Iceberg REST mock) | Yes |

## Quick start (in-process mock)

```bash
cd duckdb-iceberg/moto_s3tables
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_moto_demo.py
```

Expected: table bucket ARN, namespace `poc`, table `daily_metrics`, `list_tables` shows the table.

**Region:** the demo forces **`us-east-1`** for moto (via `MOTO_REGION`, default `us-east-1`). If your shell has `AWS_REGION=eu-west-1` from a real AWS profile, older runs hit `KeyError: 'eu-west-1'` inside moto’s s3tables backend. Do not rely on `AWS_REGION` for this mock.

Optional (shows DuckDB cannot use moto for `ENDPOINT_TYPE s3_tables`):

```bash
TRY_DUCKDB=1 python scripts/run_moto_demo.py
```

## Optional: moto server mode

```bash
# terminal 1
moto_server -p 5001

# terminal 2
MOTO_ENDPOINT=http://127.0.0.1:5001 python scripts/run_moto_server_demo.py
```

Server mode docs: https://docs.getmoto.org/en/latest/docs/server_mode.html

## When to use which

- **Learn / CI mock of S3 Tables catalog APIs** → this folder + moto.
- **DuckDB upsert demo locally** → parent `docker compose` (MinIO + Iceberg REST).
- **DuckDB against production S3 Tables** → parent `config/aws_s3_tables.example.yaml` + real AWS credentials.
