select
    technician_id,
    first_name,
    last_name,
    workshop_id,
    qualification_level,
    hourly_rate,
    hire_date,
    dbt_valid_from,
    dbt_valid_to

from {{ ref('snap_autopro_workshop__technicians') }}

where dbt_valid_to is null
