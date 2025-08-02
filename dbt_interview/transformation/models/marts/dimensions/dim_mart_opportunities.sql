{{ config(
    materialized = 'incremental',
    unique_key = ['opportunity_id',
           'opportunity_name' ],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.oppr_modified_date > dateadd(day, -7, current_date)"
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
opportunity_rec as 
(
    select *
    from {{ ref ('vw_int_opportunity') }}

     {% if is_incremental() %}

     where oppr_modified_date > (select coalesce(max(oppr_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}

),

final as 
(
    select 
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key
          ([
           'o.opportunity_id',
           'o.opportunity_name'
           ]) 
          }} as opportunity_sk,
    o.opportunity_id,
    o.account_id,
    o.opportunity_name,
    o.is_private,
    o.opportunity_description,
    o.stage_name,
    o.opportunity_type,
    --snapshot fields (flags)
    o.is_deleted as is_opportunity_deleted,
    --DWH Dates
    o.oppr_created_date,
    o.oppr_modified_date,

    current_date() as opportunity_load_date

    from opportunity_rec o

    
)

select * from final