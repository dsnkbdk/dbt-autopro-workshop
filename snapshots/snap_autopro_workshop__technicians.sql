{% snapshot snap_autopro_workshop__technicians %}

{{
    config(
        target_schema='snapshots',
        unique_key='technician_id',
        strategy='check',
        check_cols=[
            'first_name',
            'last_name',
            'workshop_id',
            'qualification_level',
            'hourly_rate',
            'hire_date'
        ]
    )
}}

select
    technician_id,
    first_name,
    last_name,
    workshop_id,
    qualification_level,
    hourly_rate,
    hire_date
from {{ ref('stg_autopro_workshop__technicians') }}

{% endsnapshot %}