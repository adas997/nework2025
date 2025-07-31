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

d.*
from dates_raw d