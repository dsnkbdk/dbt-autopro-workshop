select
  customer_id,
  address,
  city,
  postcode,
  preferred_workshop_id,
  dbt_valid_from,
  dbt_valid_to
from analytics.dbt_wshi.snap_customers
