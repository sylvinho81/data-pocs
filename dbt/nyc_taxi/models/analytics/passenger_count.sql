-- Passenger count distribution (benchmark analysis 3)
select
    passenger_count,
    count() as trip_count
from {{ source('raw', 'yellow_trip') }}
group by passenger_count
order by passenger_count
