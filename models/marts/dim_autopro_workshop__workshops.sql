select
    workshop_id,
    workshop_name,
    city,
    region,
    manager_name
from {{ ref('stg_autopro_workshop__workshops') }}
