-- Example analytics over Apache Iceberg + Postgres lookup tables via Trino
-- Prerequisite: run sql/01_setup_iceberg_silver.sql so minio_iceberg.taxi.yellow_trip_silver
-- is a real Iceberg table (see https://iceberg.apache.org/).


-- 1) Daily revenue by vendor using Postgres lookup table
select
    date_trunc('day', t.tpep_pickup_datetime) as pickup_day,
    v.description as vendor,
    sum(t.total_amount) as total_revenue
from minio_iceberg.taxi.yellow_trip_silver t
join postgres.dim.vendor v
  on t.VendorID = v.id
group by 1, 2
order by 1, 2
limit 100;


-- 2) Top pickup zones by revenue using taxi_zone lookup from Postgres
select
    z.borough,
    z.zone,
    sum(t.total_amount) as total_revenue
from minio_iceberg.taxi.yellow_trip_silver t
join postgres.dim.taxi_zone z
  on t.PULocationID = z.location_id
group by 1, 2
order by total_revenue desc
limit 50;


-- 3) Revenue share by payment type
select
    p.description as payment_type,
    sum(t.total_amount) as total_revenue
from minio_iceberg.taxi.yellow_trip_silver t
join postgres.dim.payment_type p
  on t.payment_type = p.id
group by 1
order by total_revenue desc;

