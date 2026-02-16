{% snapshot snap_technicians %}

{{
  config(
    target_database='analytics',
    target_schema='dbt_wshi',
    unique_key='technician_id',
    strategy='check',
    check_cols=['workshop_id', 'qualification_level', 'hourly_rate']
  )
}}

select
  technician_id,
  workshop_id,
  qualification_level,
  hourly_rate
from {{ ref('stg_autopro_workshop__technicians') }}

{% endsnapshot %}
