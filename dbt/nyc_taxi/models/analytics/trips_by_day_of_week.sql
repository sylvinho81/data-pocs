-- Trips grouped by day of week (Row Zero / benchmark analysis 1)
select
    dateName('weekday', tpep_pickup_datetime) as day_of_week,
    count() as trip_count
from {{ source('raw', 'yellow_trip') }}
group by day_of_week
order by day_of_week
