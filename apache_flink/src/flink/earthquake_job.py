"""
PyFlink streaming job: Kafka earthquakes → Iceberg (MinIO via REST catalog).

Highlights vs typical Spark Streaming micro-batches:
- continuous processing with short checkpoint commits to Iceberg
- event-time watermarks for late USGS updates
- exactly-once sink commits tied to Flink checkpoints

Pipeline setup order (see ``main``):
1. create streaming table environment + checkpointing
2. register Kafka source + enrichment view
3. register Iceberg REST catalog / database
4. create Iceberg sink tables
5. submit dual INSERT pipeline (raw + minute aggregates)
"""

from __future__ import annotations

import logging
import os
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("earthquake_job")


def env_or_default(name: str, default: str) -> str:
    """Return ``os.environ[name]`` if set, otherwise ``default``."""
    return os.getenv(name, default)


def create_table_env() -> StreamTableEnvironment:
    """
    Build a streaming ``StreamTableEnvironment`` with checkpointing enabled.

    Checkpoint interval (``CHECKPOINT_INTERVAL_MS``, default 30s) drives when the
    Iceberg sink commits snapshots — that is how exactly-once semantics line up
    with Kafka consumer offsets.

    Connector JARs (Kafka, Iceberg, Hadoop) must already be on Flink's classpath
    under ``/opt/flink/lib`` (see Dockerfile). Do not call ``add_jars()`` for them:
    that loads Iceberg via a ChildFirstClassLoader and collides with Flink's
    Dropwizard metrics (``LinkageError`` on ``Histogram``).
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(env_or_default("FLINK_PARALLELISM", "1")))

    checkpoint_ms = int(env_or_default("CHECKPOINT_INTERVAL_MS", "30000"))
    env.enable_checkpointing(checkpoint_ms)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    return StreamTableEnvironment.create(env, environment_settings=settings)


def register_kafka_source(t_env: StreamTableEnvironment) -> None:
    """
    Register the Kafka JSON source and an enrichment view over it.

    Creates:
    - ``earthquakes_kafka``: physical columns matching producer JSON keys, plus a
      computed ``row_time`` from ``event_time_ms`` and a 10-minute watermark so
      late USGS revisions can still join the correct event-time window.
    - ``earthquakes_enriched``: typed timestamps, magnitude bucket, and filters
      for null ids / magnitudes / event times — used by the raw Iceberg INSERT.
    """
    bootstrap = env_or_default("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = env_or_default("KAFKA_TOPIC", "earthquakes")

    t_env.execute_sql(
        f"""
        CREATE TABLE earthquakes_kafka (
            event_id        STRING,
            magnitude       DOUBLE,
            place           STRING,
            event_time      STRING,
            event_time_ms   BIGINT,
            updated_at      STRING,
            updated_ms      BIGINT,
            url             STRING,
            status          STRING,
            tsunami         INT,
            significance    INT,
            network         STRING,
            mag_type        STRING,
            event_type      STRING,
            title           STRING,
            longitude       DOUBLE,
            latitude        DOUBLE,
            depth_km        DOUBLE,
            alert           STRING,
            felt            INT,
            ingested_at     STRING,
            row_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
            WATERMARK FOR row_time AS row_time - INTERVAL '10' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = 'flink-earthquakes',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW earthquakes_enriched AS
        SELECT
            event_id,
            magnitude,
            place,
            row_time AS event_time,
            TO_TIMESTAMP_LTZ(updated_ms, 3) AS updated_at,
            url,
            status,
            tsunami,
            significance,
            network,
            mag_type,
            event_type,
            title,
            longitude,
            latitude,
            depth_km,
            alert,
            felt,
            TO_TIMESTAMP_LTZ(COALESCE(updated_ms, event_time_ms), 3) AS ingested_at,
            CASE
                WHEN magnitude < 3.0 THEN '2.5-2.9'
                WHEN magnitude < 4.0 THEN '3.0-3.9'
                WHEN magnitude < 5.0 THEN '4.0-4.9'
                WHEN magnitude < 6.0 THEN '5.0-5.9'
                ELSE '6.0+'
            END AS mag_bucket
        FROM earthquakes_kafka
        WHERE event_id IS NOT NULL
          AND magnitude IS NOT NULL
          AND event_time_ms IS NOT NULL
        """
    )


def register_iceberg_catalog(t_env: StreamTableEnvironment) -> None:
    """
    Register the Iceberg REST catalog backed by MinIO (S3-compatible storage).

    Also ensures the ``earthquakes`` database exists under ``iceberg_catalog``.
    Connection settings come from env vars so the same job works in Docker
    (service DNS) without hard-coding host ports.
    """
    rest_uri = env_or_default("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = env_or_default("ICEBERG_WAREHOUSE", "s3://warehouse/")
    s3_endpoint = env_or_default("S3_ENDPOINT", "http://minio:9000")
    access_key = env_or_default("AWS_ACCESS_KEY_ID", "admin")
    secret_key = env_or_default("AWS_SECRET_ACCESS_KEY", "password")

    t_env.execute_sql(
        f"""
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{rest_uri}',
            'warehouse' = '{warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key-id' = '{access_key}',
            's3.secret-access-key' = '{secret_key}',
            's3.path-style-access' = 'true',
            'client.region' = 'us-east-1'
        )
        """
    )
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.earthquakes")


def create_iceberg_tables(t_env: StreamTableEnvironment) -> None:
    """
    Create Iceberg sink tables if they do not already exist.

    - ``earthquakes_raw``: one row per Kafka event (append-only Parquet).
    - ``earthquakes_by_minute``: 1-minute event-time aggregates by magnitude bucket.

    Tables stay unpartitioned in this POC to keep DDL portable under Flink SQL
    (Iceberg transform partitions like ``days(ts)`` are not accepted by Flink's
    parser the same way as Spark/Trino).
    """
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg_catalog.earthquakes.earthquakes_raw (
            event_id        STRING,
            magnitude       DOUBLE,
            place           STRING,
            event_time      TIMESTAMP(3),
            updated_at      TIMESTAMP(3),
            url             STRING,
            status          STRING,
            tsunami         INT,
            significance    INT,
            network         STRING,
            mag_type        STRING,
            event_type      STRING,
            title           STRING,
            longitude       DOUBLE,
            latitude        DOUBLE,
            depth_km        DOUBLE,
            alert           STRING,
            felt            INT,
            ingested_at     TIMESTAMP(3)
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg_catalog.earthquakes.earthquakes_by_minute (
            window_start    TIMESTAMP(3),
            window_end      TIMESTAMP(3),
            mag_bucket      STRING,
            quake_count     BIGINT,
            max_magnitude   DOUBLE
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )


def start_pipeline(t_env: StreamTableEnvironment) -> None:
    """
    Wire Kafka → Iceberg as one Flink job with two continuous INSERTs.

    Uses a ``StatementSet`` so both sinks share the same streaming graph and
    checkpoint barrier (raw events + tumble-window counts).

    Does not call ``wait()`` on the result: ``flink run -d`` can detach after the
    JobManager accepts the job.
    """
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW quakes_for_window AS
        SELECT
            magnitude,
            row_time,
            CASE
                WHEN magnitude < 3.0 THEN '2.5-2.9'
                WHEN magnitude < 4.0 THEN '3.0-3.9'
                WHEN magnitude < 5.0 THEN '4.0-4.9'
                WHEN magnitude < 6.0 THEN '5.0-5.9'
                ELSE '6.0+'
            END AS mag_bucket
        FROM earthquakes_kafka
        WHERE event_id IS NOT NULL
          AND magnitude IS NOT NULL
          AND event_time_ms IS NOT NULL
        """
    )

    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql(
        """
        INSERT INTO iceberg_catalog.earthquakes.earthquakes_raw
        SELECT
            event_id,
            magnitude,
            place,
            event_time,
            updated_at,
            url,
            status,
            tsunami,
            significance,
            network,
            mag_type,
            event_type,
            title,
            longitude,
            latitude,
            depth_km,
            alert,
            felt,
            ingested_at
        FROM earthquakes_enriched
        """
    )

    stmt_set.add_insert_sql(
        """
        INSERT INTO iceberg_catalog.earthquakes.earthquakes_by_minute
        SELECT
            TUMBLE_START(row_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(row_time, INTERVAL '1' MINUTE) AS window_end,
            mag_bucket,
            COUNT(*) AS quake_count,
            MAX(magnitude) AS max_magnitude
        FROM quakes_for_window
        GROUP BY TUMBLE(row_time, INTERVAL '1' MINUTE), mag_bucket
        """
    )

    logger.info("Submitting Kafka → Iceberg streaming pipeline")
    stmt_set.execute()


def main() -> None:
    """Entry point: configure Flink SQL objects, then submit the streaming job."""
    t_env = create_table_env()
    register_kafka_source(t_env)
    register_iceberg_catalog(t_env)
    create_iceberg_tables(t_env)
    start_pipeline(t_env)


if __name__ == "__main__":
    main()
