with tx as (
  select * from {{ ref('int_tx_deduped') }}
),

cust as (
  select * from {{ ref('dim_customers_scd2') }}
)

select
  tx.*,
  cust.address as customer_address_at_service,
  cust.city as customer_city_at_service,
  cust.postcode as customer_postcode_at_service,
  cust.preferred_workshop_id as customer_preferred_workshop_at_service
from tx
left join cust
  on tx.customer_id = cust.customer_id
 and tx.transaction_date >= cust.dbt_valid_from
 and (tx.transaction_date < cust.dbt_valid_to or cust.dbt_valid_to is null)
