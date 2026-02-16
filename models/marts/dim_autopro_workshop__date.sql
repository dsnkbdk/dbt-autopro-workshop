{{ config(static_analysis = 'unsafe') }}

with date_spine as (
    
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="to_date('2015-01-01')",
            end_date="to_date('2035-12-31')"
        )
    }}
),

final as (
    select
        date_day,
        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,
        day(date_day) as day_of_month,
        dayofweekiso(date_day) as day_of_week_iso,
        dayname(date_day) as day_name,
        weekofyear(date_day) as week_of_year,
        date_trunc('month', date_day) as month_start,
        last_day(date_day, 'month') as month_end
  
  from date_spine
)

select * from final
