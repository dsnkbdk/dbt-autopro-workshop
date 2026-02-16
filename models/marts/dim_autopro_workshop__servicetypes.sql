select
    service_type_id,
    service_type_name,
    category
from {{ ref('stg_autopro_workshop__servicetypes') }}