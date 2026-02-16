{% snapshot snap_autopro_workshop__vehicles %}

{{
    config(
        target_schema='snapshots',
        unique_key='vehicle_id',
        strategy='check',
        check_cols=[
            'customer_id',
            'make',
            'model',
            'year',
            'rego',
            'odometer',
            'color'
        ]
    )
}}

select
    vehicle_id,
    customer_id,
    make,
    model,
    year,
    rego,
    odometer,
    color
from {{ ref('stg_autopro_workshop__vehicles') }}

{% endsnapshot %}