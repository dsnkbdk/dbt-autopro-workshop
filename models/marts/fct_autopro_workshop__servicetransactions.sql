{{
    config(
        materialized='incremental',
        unique_key = 'transaction_id',
        incremental_strategy = 'merge'
    )
}}


{% set lookback_days = 3 %}


with

servicetransactions as (
    select * from {{ ref('stg_autopro_workshop__servicetransactions') }}
),

dim_customers as (
    select * from {{ ref('dim_autopro_workshop__customers') }}
),

dim_technicians as (
  select * from {{ ref('dim_autopro_workshop__technicians') }}
),

dim_vehicles as (
  select * from {{ ref('dim_autopro_workshop__vehicles') }}
),

dim_servicetypes as (
  select * from {{ ref('dim_autopro_workshop__servicetypes') }}
),

dim_workshops as (
  select * from {{ ref('dim_autopro_workshop__workshops') }}
),

final as (
    select
        st.transaction_id,
        st.transaction_date,
        st.modified_date,

        c.customer_id,
        t.technician_id,
        v.vehicle_id,
        w.workshop_id,
        s.service_type_id,

        st.labour_hours,
        st.labour_cost,
        st.parts_cost,
        st.total_cost,
        st.payment_method,
        st.status,

        st._etl_loaded_at,
        st._etl_file_name,
        st._etl_row_number

    from servicetransactions st

    left join dim_customers c on st.customer_id = c.customer_id
    left join dim_technicians t on st.technician_id = t.technician_id
    left join dim_vehicles v on st.vehicle_id = v.vehicle_id
    left join dim_servicetypes s on st.service_type_id = s.service_type_id
    left join dim_workshops w   on st.workshop_id = w.workshop_id
)

select * from final

{% if is_incremental() %}
    where modified_date >= dateadd(
        day,
        -{{ lookback_days }},
        (select max(modified_date) from {{ this }})
)
{% endif %}
