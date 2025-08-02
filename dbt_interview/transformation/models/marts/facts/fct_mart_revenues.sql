{{ config(
    materialized = 'incremental',
    unique_key = ['account_id','opportunity_id'],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.account_load_date > dateadd(day, -7, current_date)
      or DBT_INTERNAL_DEST.opportunity_load_date > dateadd(day, -7, current_date)"
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
dim_opportunity_rec as 
(
    select *
    from {{ ref ('dim_mart_opportunities') }}

    {% if is_incremental() %}

     where oppr_modified_date > (select coalesce(max(oppr_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
final as 
(
select 
      dim_acc.account_id,
      dim_opp.opportunity_id,
      sum(a.annual_revenue) as total_revenue_earned,
      sum(o.amount) as total_opportunity_amount,
      sum(o.expected_revenue) as total_revenue_expected,
      avg(o.probability) as average_probability,
      dim_acc.account_load_date,
      dim_opp.opportunity_load_date,
      
from account_rec  a 
inner join dim_acc_data dim_acc on (a.account_id = dim_acc.account_id)
left join opportunity_rec o on (o.account_id = a.account_id)
inner join dim_opportunity_rec dim_opp on (o.opportunity_id = dim_opp.opportunity_id
                                           and dim_opp.account_id = dim_acc.account_id)                       
where dim_acc.is_account_deleted = 1
and dim_opp.is_opportunity_deleted = 1
group by       
      dim_acc.account_id,
      dim_opp.opportunity_id,
      dim_acc.account_load_date,
      dim_opp.opportunity_load_date
)
select 
       
      {{ dbt_utils.generate_surrogate_key
          (['account_load_date','opportunity_load_date'     
           ]) 
          }} as date_sk,
  account_id,
  opportunity_id,
  {{ cents_to_dollars('total_revenue_earned') }} as total_revenue_earned_usd,
  {{ cents_to_dollars('total_opportunity_amount') }} as total_opportunity_amount_usd,
  {{ cents_to_dollars('total_revenue_expected') }} as total_revenue_expected_usd,
  average_probability,
  account_load_date,
  opportunity_load_date

  from final







