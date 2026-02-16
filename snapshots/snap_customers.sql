{% snapshot snap_customers %}

{{
  config(
    target_database='analytics',
    target_schema='dbt_wshi',
    unique_key='customer_id',
    strategy='check',
    check_cols=['address', 'city', 'postcode', 'preferred_workshop_id']
  )
}}

select
  customer_id,
  address,
  city,
  postcode,
  preferred_workshop_id
from {{ ref('stg_autopro_workshop__customers') }}

{% endsnapshot %}
