with date_spine as (

    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="(select date_sub(least(coalesce(start_date, current_date()), coalesce(end_date, current_date())), interval 30 day)
                     from " ~ ref('stg_summary_date_range') ~ ")",
        end_date="(select date_add(greatest(coalesce(start_date, current_date()), coalesce(end_date, current_date())), interval 30 day)
                     from " ~ ref('stg_summary_date_range') ~ ")"
    ) }}

)

select
    cast(format_date('%Y%m%d', date_day) as int64) as date_key,
    date_day as full_date,
    extract(day from date_day) as day_of_month,
    extract(month from date_day) as month_of_year,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    format_date('%A', date_day) as day_name,
    format_date('%a', date_day) as day_name_short,
    format_date('%B', date_day) as month_name,
    format_date('%b', date_day) as month_name_short,
    extract(dayofweek from date_day) as day_of_week,
    extract(week from date_day) as calendar_week,
    case when extract(dayofweek from date_day) in (1,7) then true else false end as is_weekend
from date_spine
