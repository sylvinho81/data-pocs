-- Setup raw (Hive external) and silver (Apache Iceberg) tables.
-- Run after: docker compose up, python etl_minio_iceberg.py (uploads raw parquet to MinIO).
-- See https://iceberg.apache.org/

-- ---------------------------------------------------------------------------
-- 1) Raw layer: Hive external table over parquet files in MinIO (taxi-raw bucket)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS minio_raw.raw
WITH (location = 's3a://taxi-raw/');

CREATE TABLE IF NOT EXISTS minio_raw.raw.yellow_trip_raw (
    VendorID bigint,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance double,
    RatecodeID bigint,
    PULocationID bigint,
    DOLocationID bigint,
    payment_type bigint,
    fare_amount double,
    tip_amount double,
    total_amount double
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://taxi-raw/ny_taxi_raw/'
);

-- ---------------------------------------------------------------------------
-- 2) Silver layer: Apache Iceberg table (metadata + Parquet data in MinIO)
--    Trino creates real Iceberg metadata (metadata.json, manifests) and data files.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS minio_iceberg.taxi
WITH (location = 's3a://trino-poc-iceberg/taxi/');

-- Iceberg table: metadata (metadata.json, manifests) is Iceberg; data files are Parquet.
CREATE TABLE IF NOT EXISTS minio_iceberg.taxi.yellow_trip_silver (
    VendorID bigint,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance double,
    RatecodeID bigint,
    PULocationID bigint,
    DOLocationID bigint,
    payment_type bigint,
    fare_amount double,
    tip_amount double,
    total_amount double
)
WITH (
    format = 'PARQUET'  -- data file format; table format is Iceberg (catalog minio_iceberg)
);

-- Load silver from raw: writes proper Iceberg table (metadata + Parquet files).
-- To reload from scratch: DROP TABLE minio_iceberg.taxi.yellow_trip_silver; then re-run from CREATE TABLE.
--
-- NOTE: Loading *all* raw files in one INSERT can be heavy for a single-node Trino POC and may cause restarts.
-- Start with a smaller load, verify everything works, then increase/remove the LIMIT.
INSERT INTO minio_iceberg.taxi.yellow_trip_silver
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM minio_raw.raw.yellow_trip_raw
LIMIT 1000000;
