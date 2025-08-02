{{ config(
    materialized = 'incremental',
    unique_key = ['product_id',
           'pricebook_entry_id',
           'pricebook_id' ],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.prod_modified_date > dateadd(day, -7, current_date)"
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

prod_rec as 
(
    select *
    from {{ ref('vw_int_product') }}

    {% if is_incremental() %}

     where prod_modified_date > (select coalesce(max(prod_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}

),
final as 
(
select 
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key
          ([
           'p.product_id'
           ]) 
          }} as prod_sk,

   p.product_id,
   p.product_code,
   p.product_type,
   p.product_class,
   p.product_description,
   p.quantity_unit,


-- DWH Dates
   p.prod_created_date,
   p.prod_modified_date ,
   
-- Snapshot Dates
   
   p.is_deleted as is_prod_deleted,

   current_date() as prod_load_date

   from prod_rec p 
   



)
select * from final