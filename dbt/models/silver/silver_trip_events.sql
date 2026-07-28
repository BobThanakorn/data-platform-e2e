with calculated as (
    select
        sha256(concat_ws(
            '|',
            coalesce(cast(vendor_id as varchar), ''),
            coalesce(cast(pickup_at as varchar), ''),
            coalesce(cast(dropoff_at as varchar), ''),
            coalesce(cast(pickup_zone_id as varchar), ''),
            coalesce(cast(dropoff_zone_id as varchar), ''),
            coalesce(cast(total_amount as varchar), '')
        )) as trip_id,
        *,
        date_diff('second', pickup_at, dropoff_at) / 60.0 as duration_minutes,
        case
            when date_diff('second', pickup_at, dropoff_at) > 0
                then trip_distance_miles / (date_diff('second', pickup_at, dropoff_at) / 3600.0)
            else null
        end as average_speed_mph
    from {{ ref('stg_trips') }}
),
scored as (
    select
        *,
        case
            when pickup_at is null or dropoff_at is null then 'missing_timestamp'
            when dropoff_at <= pickup_at then 'invalid_duration'
            when duration_minutes > 240 then 'duration_out_of_range'
            when trip_distance_miles < 0 or trip_distance_miles > 200 then 'distance_out_of_range'
            when total_amount < 0 or total_amount > 1000 then 'amount_out_of_range'
            when pickup_zone_id is null or dropoff_zone_id is null then 'missing_zone'
            else 'accepted'
        end as quality_status
    from calculated
),
deduplicated as (
    select
        *,
        row_number() over (partition by trip_id order by transformed_at desc) as duplicate_rank
    from scored
)

select * exclude (duplicate_rank)
from deduplicated
where duplicate_rank = 1
