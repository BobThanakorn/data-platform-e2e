select *
from {{ ref('silver_trip_events') }}
where quality_status <> 'accepted'
