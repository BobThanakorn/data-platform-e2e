{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where dataset_month = '{{ var('dataset_month') }}'{% endif %}"
    )
}}

select *
from {{ ref('silver_trip_events') }}
where quality_status = 'accepted'
