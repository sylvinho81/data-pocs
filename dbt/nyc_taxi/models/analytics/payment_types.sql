-- Payment type counts and revenue (benchmark analysis 2)
select
    payment_type,
    count() as count,
    sum(total_amount) as total_revenue
from {{ source('raw', 'yellow_trip') }}
group by payment_type
order by payment_type
