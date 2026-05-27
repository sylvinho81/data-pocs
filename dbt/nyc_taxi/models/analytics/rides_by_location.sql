-- Top 20 pickup locations by trip count (benchmark analysis 6)
select
    pu_location_id as pulocation_id,
    count() as trip_count
from {{ source('raw', 'yellow_trip') }}
group by pu_location_id
order by trip_count desc
limit 20
