select
  transaction_id,
  transaction_date,
  modified_date,
  _etl_loaded_at,

  workshop_id,
  service_type_id,
  vehicle_id,

  customer_id,
  customer_address_at_service,
  customer_city_at_service,
  customer_postcode_at_service,
  customer_preferred_workshop_at_service,

  technician_id,
  technician_workshop_at_service,
  technician_qualification_at_service,
  technician_hourly_rate_at_service,

  labour_hours,
  labour_cost,
  parts_cost,
  total_cost,
  payment_method,
  status,

  (labour_cost + parts_cost) as recomputed_total_cost,
  case
    when total_cost is null then null
    when abs(total_cost - (labour_cost + parts_cost)) <= 0.01 then true
    else false
  end as is_total_cost_consistent
from {{ ref('int_tx_technician_asof') }}
