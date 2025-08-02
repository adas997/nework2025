{{ config(
    materialized = 'incremental',
    unique_key = ['product_id','product_code','case_id','account_id'],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.prod_load_date > dateadd(day, -7, current_date)"
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

with prod_data
as
(
    select *
    from {{ ref('dim_mart_products')}}

    {% if is_incremental() %}

     where prod_modified_date > (select coalesce(max(prod.prod_modified_date),'1900-01-01') from {{this}} prod )


     {% endif%}
),
price_data as
(
    select *
    from {{ ref('vw_int_price_book_entry')}}

    {% if is_incremental() %}

     where price_modified_date > (select coalesce(max(price.price_modified_date),'1900-01-01') from {{this}} price )


     {% endif%}

),

case_data as
(
    select *
    from {{ ref('dim_mart_cases')}}

    {% if is_incremental() %}

     where case_modified_date > (select coalesce(max(cases.case_modified_date),'1900-01-01') from {{this}} cases )


     {% endif%}

),

final as 

(

select p.product_id,
    c.case_id,
    c.account_id,
    min(pr.unit_price) as min_unit_price,
    max(pr.unit_price) as max_unit_price,
    min(pr.use_standard_price) as min_use_standard_price,
    max(pr.use_standard_price) as max_use_standard_price,
    sum(pr.unit_price + pr.use_standard_price) as total_unit_price,
    p.prod_load_date
from prod_data p
    left join price_data pr on (pr.product_id = p.product_id)
    left join case_data c on (p.product_id = c.product_id)
where is_prod_deleted = 1
    and pr.is_deleted = 1
    and c.is_deleted = 1
group by p.product_id,
    c.case_id,
    c.account_id,
    p.prod_load_date

)

select 
       
      {{ dbt_utils.generate_surrogate_key
          (['prod_load_date'    
           ]) 
          }} as date_sk,
  product_id,
  case_id,
  account_id,
  {{ cents_to_dollars('min_unit_price') }} as min_unit_price_usd,
  {{ cents_to_dollars('max_unit_price') }} as max_unit_price_usd,
  {{ cents_to_dollars('min_use_standard_price') }} as min_use_standard_price_usd,
  {{ cents_to_dollars('max_use_standard_price') }} as max_use_standard_price_usd,
  {{ cents_to_dollars('total_unit_price') }} as total_unit_price_usd,
  prod_load_date

  from final
           

