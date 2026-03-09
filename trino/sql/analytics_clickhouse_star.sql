-- Example analytics over ClickHouse star schema via Trino

-- 1) Daily revenue by vendor
select
    date_trunc('day', f.pickup_datetime) as pickup_day,
    v.description as vendor,
    sum(f.total_amount) as total_revenue
from clickhouse.taxi.fact_yellow_trip f
join clickhouse.taxi.dim_vendor v
  on f.vendor_id = v.id
group by 1, 2
order by 1, 2
limit 100;


-- 2) Revenue by borough and zone
select
    z.borough,
    z.zone,
    sum(f.total_amount) as total_revenue
from clickhouse.taxi.fact_yellow_trip f
join clickhouse.taxi.dim_taxi_zone z
  on f.pulocation_id = z.location_id
group by 1, 2
order by total_revenue desc
limit 50;


-- 3) Payment type mix
select
    p.description as payment_type,
    sum(f.total_amount) as total_revenue
from clickhouse.taxi.fact_yellow_trip f
join clickhouse.taxi.dim_payment_type p
  on f.payment_type = p.id
group by 1
order by total_revenue desc;

