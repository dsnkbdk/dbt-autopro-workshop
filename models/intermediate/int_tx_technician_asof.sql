with tx as (
  select * from {{ ref('int_tx_customer_asof') }}
),

tech as (
  select * from {{ ref('dim_technicians_scd2') }}
)

select
  tx.*,
  tech.workshop_id as technician_workshop_at_service,
  tech.qualification_level as technician_qualification_at_service,
  tech.hourly_rate as technician_hourly_rate_at_service
from tx
left join tech
  on tx.technician_id = tech.technician_id
 and tx.transaction_date >= tech.dbt_valid_from
 and (tx.transaction_date < tech.dbt_valid_to or tech.dbt_valid_to is null)
