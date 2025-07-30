  {{
      config(
        materialized = 'table'
      )
  }}
with log_data as 
(select cast(NULL as varchar ) as model_name,
        cast(NULL as timestamp) as run_time,
        cast(NULL as bigint) as row_count
        )
select * from log_data