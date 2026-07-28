with source as (
    select *
    from read_parquet('{{ var("bronze_trip_path") }}')
)

select
    '{{ var("dataset_month") }}' as dataset_month,
    cast(VendorID as integer) as vendor_id,
    timezone('America/New_York', cast(tpep_pickup_datetime as timestamp)) as pickup_at,
    timezone('America/New_York', cast(tpep_dropoff_datetime as timestamp)) as dropoff_at,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance_miles,
    cast(RatecodeID as integer) as rate_code_id,
    cast(store_and_fwd_flag as varchar) as store_and_forward_flag,
    cast(PULocationID as integer) as pickup_zone_id,
    cast(DOLocationID as integer) as dropoff_zone_id,
    cast(payment_type as integer) as payment_type_id,
    cast(fare_amount as decimal(18, 2)) as fare_amount,
    cast(extra as decimal(18, 2)) as extra_amount,
    cast(mta_tax as decimal(18, 2)) as mta_tax_amount,
    cast(tip_amount as decimal(18, 2)) as tip_amount,
    cast(tolls_amount as decimal(18, 2)) as tolls_amount,
    cast(improvement_surcharge as decimal(18, 2)) as improvement_surcharge_amount,
    cast(total_amount as decimal(18, 2)) as total_amount,
    cast(congestion_surcharge as decimal(18, 2)) as congestion_surcharge_amount,
    current_timestamp as transformed_at
from source
