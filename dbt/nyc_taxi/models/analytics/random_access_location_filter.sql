-- Random access: trips matching 50 sampled location IDs (benchmark analysis 7)
select
    count() as count
from {{ source('raw', 'yellow_trip') }} as trips
where trips.pu_location_id in (
    select location_id
    from {{ ref('random_sample_location_ids') }}
)
