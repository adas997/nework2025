{ { config(materialized = 'table') } } 
with log_data as (
    select cast(NULL as varchar) as model_name
)
select *
from log_data