{{ config(materialized='view') }}

select *
from {{ ref('fact_trips') }}
