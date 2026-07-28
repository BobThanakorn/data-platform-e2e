{{
    config(
        materialized='incremental',
        unique_key=['dataset_month', 'weekday_number', 'pickup_hour'],
        incremental_strategy='delete+insert',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where dataset_month = '{{ var('dataset_month') }}'{% endif %}"
    )
}}

select
    strftime(pickup_at, '%Y-%m') as dataset_month,
    dayname(pickup_at) as weekday_name,
    dayofweek(pickup_at) as weekday_number,
    hour(pickup_at) as pickup_hour,
    count(*) as trip_count,
    round(avg(fare_amount), 2) as average_fare_amount,
    round(avg(duration_minutes), 2) as average_duration_minutes,
    round(100.0 * avg(case when tip_amount > 0 then 1 else 0 end), 2) as tip_rate_pct
from {{ ref('fact_trips') }}
where dataset_month = '{{ var("dataset_month") }}'
  and strftime(pickup_at, '%Y-%m') = '{{ var("dataset_month") }}'
group by 1, 2, 3, 4
