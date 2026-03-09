from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PARQUET_DIR = PROJECT_ROOT.parent / "trino" / "parquet_files" / "ny_taxi_raw" 
RAW_TAXI_ZONE_LOOKUP = PROJECT_ROOT / "raw_files" / "taxi_zone_lookup.csv"

POSTGRES_DSN = "postgresql://trino:trino@localhost:5433/taxi"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False
MINIO_RAW_BUCKET = "taxi-raw"
MINIO_ICEBERG_BUCKET = "trino-poc-iceberg"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USERNAME = "trino"
CLICKHOUSE_PASSWORD = "trino"
CLICKHOUSE_DATABASE = "taxi"

