{{ config(materialized='view') }}

-- Pickup hour distribution (materialized as a ClickHouse view, not a table)
select
    toHour(tpep_pickup_datetime) as hour_of_day,
    count() as trip_count
from {{ source('raw', 'yellow_trip') }}
group by hour_of_day
order by hour_of_day
