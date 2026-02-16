with spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="to_date('2024-01-01')",
      end_date="dateadd(day, 365, current_date())"
  ) }}
)
select
  date_day as date,
  year(date_day) as year,
  month(date_day) as month,
  quarter(date_day) as quarter,
  dayofweekiso(date_day) as dow_iso,
  case when dayofweekiso(date_day) in (6,7) then true else false end as is_weekend
from spine
