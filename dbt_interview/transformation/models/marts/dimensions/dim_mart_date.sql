{{
    config(
        materialized = "table"
    )
}}


with 
    dates_raw as (
    
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1970-01-01' as date)",
        end_date="date_add(current_date(), interval 100 year)"
        )
    }}
)
select {{ dbt_utils.generate_surrogate_key
          (['date(d.date_day)']) 
          }} as dim_date_sk,

          date_day::date as date_day,
          extract(isodow from date_day) as day_of_week_number,
          extract(day from date_day) as day_of_month_number,
          extract(doy from date_day) as day_of_year_number,
          extract(week from date_day) as week_of_year_number,
          extract(month from date_day) as month_of_year_number,
          extract(quarter from date_day) as quarter_of_year_number,
          extract(year from date_day) as year_number,

        --  datepart(date_day::date,'Dy') as short_weekday_name,
        --  datepart(date_day::date,'Day') as full_weekday_name,
        --  datepart(date_day::date,'Mon') as short_month_name,
        --  datepart(date_day::date,'Month') as full_month_name

        strftime(date_day::date,'%a') as short_weekday_name,
        datepart('Day',date_day::date) as full_weekday_name,
        datepart('Mon',date_day::date) as short_month_name,
        datepart('Month',date_day::date) as full_month_name,
          
        concat(datepart('Mon',date_day::date),' ',extract(year from date_day)) as short_month_year,
        concat(datepart('Month',date_day::date),' ',extract(year from date_day)) as full_month_year

--d.*
from dates_raw d