{% snapshot snap_autopro_workshop__customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'first_name',
            'last_name',
            'email',
            'phone',
            'address',
            'city',
            'postcode',
            'preferred_workshop'
        ]
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    postcode,
    preferred_workshop
from {{ ref('stg_autopro_workshop__customers') }}

{% endsnapshot %}
