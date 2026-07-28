with duplicates as (
    select 'mart_zone_performance' as model_name
    from {{ ref('mart_zone_performance') }}
    group by trip_date, zone_id
    having count(*) > 1

    union all

    select 'mart_hourly_pattern' as model_name
    from {{ ref('mart_hourly_pattern') }}
    group by dataset_month, weekday_number, pickup_hour
    having count(*) > 1
)

select * from duplicates
