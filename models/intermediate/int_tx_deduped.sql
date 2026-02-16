with tx as (
  select * from {{ ref('stg_autopro_workshop__servicetransactions') }}
),

ranked as (
  select
    *,
    row_number() over (
      partition by transaction_id
      order by
        coalesce(modified_date, transaction_date) desc,
        _etl_loaded_at desc
    ) as rn
  from tx
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
  _etl_loaded_at
from ranked
where rn = 1
