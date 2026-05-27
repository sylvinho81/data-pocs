-- Monthly rides and congestion fee impact (benchmark analysis 4)
select
    formatDateTime(tpep_pickup_datetime, '%Y-%m') as month,
    count() as total_rides,
    sum(cbd_congestion_fee) as total_congestion_fee,
    countIf(cbd_congestion_fee > 0) as rides_with_congestion_fee
from {{ source('raw', 'yellow_trip') }}
group by month
order by month
