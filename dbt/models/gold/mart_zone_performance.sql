{{
    config(
        materialized='incremental',
        unique_key=['trip_date', 'zone_id'],
        incremental_strategy='delete+insert',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where strftime(trip_date, '%Y-%m') = '{{ var('dataset_month') }}'{% endif %}"
    )
}}

select
    cast(t.pickup_at as date) as trip_date,
    t.pickup_zone_id as zone_id,
    z.borough,
    z.zone_name,
    count(*) as trip_count,
    round(sum(t.total_amount), 2) as revenue_amount,
    round(avg(t.fare_amount), 2) as average_fare_amount,
    round(avg(t.duration_minutes), 2) as average_duration_minutes,
    round(avg(t.average_speed_mph), 2) as average_speed_mph
from {{ ref('fact_trips') }} as t
left join {{ ref('dim_zone') }} as z
    on t.pickup_zone_id = z.zone_id
where t.dataset_month = '{{ var("dataset_month") }}'
  and strftime(t.pickup_at, '%Y-%m') = '{{ var("dataset_month") }}'
group by 1, 2, 3, 4
