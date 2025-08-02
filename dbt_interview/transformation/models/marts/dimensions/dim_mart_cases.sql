{{ config(
    materialized = 'incremental',
    unique_key = ['case_id',
           'case_number' ],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.case_modified_date > dateadd(day, -7, current_date)"
    ],
    post_hook = [
            """
            insert into main.log_model_run_details
            select '{{this.name}}' as model_name,
            now() as run_time,
            count(*) as row_count
            from {{this}}            
            """
        ]
) }}

with 
case_rec as 
(
    select *
    from {{ ref ('vw_int_case') }}

     {% if is_incremental() %}

     where case_modified_date > (select coalesce(max(case_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}

)
select 
-- Surrogate Key
    {{ dbt_utils.generate_surrogate_key
          ([
           'cs.case_id',
           'cs.case_number'
           ]) 
          }} as case_sk,
cs.*
from case_rec cs