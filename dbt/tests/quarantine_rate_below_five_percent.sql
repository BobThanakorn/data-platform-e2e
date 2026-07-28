with counts as (
    select
        count(*) as total_rows,
        sum(case when quality_status <> 'accepted' then 1 else 0 end) as rejected_rows
    from {{ ref('silver_trip_events') }}
)

select *
from counts
where rejected_rows / greatest(total_rows, 1) > 0.05
