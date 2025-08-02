{{ config(
    materialized = 'incremental',
    unique_key = ['dim_acc.acc_opp_con_case_prod_sk','u.user_id'],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.load_date > dateadd(day, -7, current_date)"
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
with dim_acc_data as
(
    select *
    from {{ ref ('dim_mart_accounts') }}

    {% if is_incremental() %}

     where load_date > (select coalesce(max(load_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
account_rec as 
(
    select *
     from {{ ref('vw_int_account') }}

     {% if is_incremental() %}

     where acc_modified_date > (select coalesce(max(acc.acc_modified_date),'1900-01-01') from {{this}} acc )


     {% endif%}
),
opportunity_rec as 
(
    select *
    from {{ ref ('vw_int_opportunity') }}

     {% if is_incremental() %}

     where oppr_modified_date > (select coalesce(max(oppr_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
price_rec as 
(
    select *
    from {{ ref ('vw_int_price_book_entry') }}

    {% if is_incremental() %}

     where price_modified_date > (select coalesce(max(price_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
user_rec as 
(
    select *
    from {{ ref ('vw_int_user') }}

    {% if is_incremental() %}

     where user_modified_date > (select coalesce(max(user_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
final as 
(
select 
      {{ dbt_utils.generate_surrogate_key
          (['dim_acc.load_date'     
           ]) 
          }} as date_sk,
      dim_acc.acc_opp_con_case_prod_sk,
      dim_acc.account_id,
      dim_acc.opportunity_id,
      dim_acc.contact_id,
      dim_acc.case_id,
      dim_acc.product_id,
      dim_acc.pricebook_id,
      u.user_id,
      sum(a.annual_revenue) as total_revenue_earned,
      sum(o.amount) as total_opportunity_amount,
      sum(o.expected_revenue) as total_revenue_expected,
      avg(o.probability) as average_probability,
      min(pr.unit_price) as min_unit_price,
      max(pr.unit_price) as max_unit_price,
      min(pr.use_standard_price) as min_use_standard_price,
      max(pr.use_standard_price) as max_use_standard_price,
      sum(pr.unit_price + pr.use_standard_price) as total_unit_price
from dim_acc_data as dim_acc 
left join account_rec a on (a.account_id = dim_acc.account_id)
left join opportunity_rec o on (o.opportunity_id = dim_acc.opportunity_id)
left join price_rec pr on (pr.pricebook_entry_id = dim_acc.pricebook_entry_id 
                          and pr.pricebook_id = dim_acc.pricebook_id)
left join user_rec u on (a.account_id = u.account_id 
                           and u.contact_id  = dim_acc.contact_id)                         
where dim_acc.is_account_deleted = 1
and dim_acc.is_opportunity_deleted = 1
and dim_acc.is_contact_deleted = 1
and dim_acc.is_case_deleted = 1
and dim_acc.is_prod_deleted = 1
and dim_acc.is_price_deleted = 1
group by 
      date_sk,
      dim_acc.acc_opp_con_case_prod_sk,
      dim_acc.account_id,
      dim_acc.opportunity_id,
      dim_acc.contact_id,
      dim_acc.case_id,
      dim_acc.product_id,
      dim_acc.pricebook_id,
      u.user_id
)
select 
  date_sk,
  acc_opp_con_case_prod_sk,
  account_id,
  opportunity_id,
  contact_id,
  case_id,
  product_id,
  pricebook_id,
  {{ cents_to_dollars('total_revenue_earned') }} as total_revenue_earned_usd,
  {{ cents_to_dollars('total_opportunity_amount') }} as total_opportunity_amount_usd,
  {{ cents_to_dollars('total_revenue_earned') }} as total_revenue_earned_usd,
  average_probability,
  {{ cents_to_dollars('min_unit_price') }} as min_unit_price_usd,
  {{ cents_to_dollars('max_unit_price') }} as max_unit_price_usd,
  {{ cents_to_dollars('min_use_standard_price') }} as min_use_standard_price_usd,
  {{ cents_to_dollars('max_use_standard_price') }} as max_use_standard_price_usd,
  {{ cents_to_dollars('total_unit_price') }} as total_unit_price_usd

  from final







