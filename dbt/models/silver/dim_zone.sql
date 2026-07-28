select distinct
    zone_id,
    borough,
    zone_name,
    service_zone
from {{ ref('stg_zones') }}
where zone_id is not null
