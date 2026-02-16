{{
    config(
        materialized='incremental',
        unique_key = 'transaction_id',
        incremental_strategy = 'merge'
    )
}}

{% set lookback_days = 3 %}

with src as (
    select * from {{ ref('stg_autopro_workshop__servicetransactions') }}
)

select
    transaction_id,
    transaction_date,
    workshop_id,
    customer_id,
    vehicle_id,
    technician_id,
    service_type_id,
    labour_hours,
    labour_cost,
    parts_cost,
    total_cost,
    payment_method,
    status,
    modified_date,
    _etl_loaded_at,
    _etl_file_name,
    _etl_row_number

from src

{% if is_incremental() %}
    where modified_date >= dateadd(
        day,
        -{{ lookback_days }},
        (select max(modified_date) from {{ this }})
)
{% endif %}
