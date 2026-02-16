select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    postcode,
    preferred_workshop,
    dbt_valid_from,
    dbt_valid_to

from {{ ref('snap_autopro_workshop__customers') }}

where dbt_valid_to is null
