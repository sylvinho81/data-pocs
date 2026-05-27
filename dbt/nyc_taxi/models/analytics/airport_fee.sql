-- Airport fee percentage (benchmark analysis 5)
select
    count() as total_rides,
    countIf(airport_fee > 0) as rides_with_airport_fee,
    countIf(airport_fee > 0) * 100.0 / count() as percentage_with_airport_fee
from {{ source('raw', 'yellow_trip') }}
