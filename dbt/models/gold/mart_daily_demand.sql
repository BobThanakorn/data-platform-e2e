{{
    config(
        materialized='incremental',
        unique_key='trip_date',
        incremental_strategy='delete+insert',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where strftime(trip_date, '%Y-%m') = '{{ var('dataset_month') }}'{% endif %}"
    )
}}

select
    cast(pickup_at as date) as trip_date,
    count(*) as trip_count,
    sum(coalesce(passenger_count, 0)) as passenger_count,
    round(sum(trip_distance_miles), 2) as distance_miles,
    round(sum(total_amount), 2) as revenue_amount,
    round(avg(fare_amount), 2) as average_fare_amount,
    round(avg(duration_minutes), 2) as average_duration_minutes,
    round(100.0 * avg(case when tip_amount > 0 then 1 else 0 end), 2) as tip_rate_pct
from {{ ref('fact_trips') }}
where dataset_month = '{{ var("dataset_month") }}'
  and strftime(pickup_at, '%Y-%m') = '{{ var("dataset_month") }}'
group by 1
