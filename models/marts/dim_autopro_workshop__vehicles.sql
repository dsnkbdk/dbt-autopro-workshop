select
    vehicle_id,
    customer_id,
    make,
    model,
    year,
    rego,
    odometer,
    color,
    dbt_valid_from,
    dbt_valid_to

from {{ ref('snap_autopro_workshop__vehicles') }}

where dbt_valid_to is null
