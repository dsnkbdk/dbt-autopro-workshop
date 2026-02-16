select
  technician_id,
  workshop_id,
  qualification_level,
  hourly_rate,
  dbt_valid_from,
  dbt_valid_to
from analytics.dbt_wshi.snap_technicians
