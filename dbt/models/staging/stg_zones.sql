select
    cast(LocationID as integer) as zone_id,
    cast(Borough as varchar) as borough,
    cast(Zone as varchar) as zone_name,
    cast(service_zone as varchar) as service_zone
from read_csv_auto(
    '{{ var("bronze_zone_path") }}',
    header = true,
    all_varchar = true
)
