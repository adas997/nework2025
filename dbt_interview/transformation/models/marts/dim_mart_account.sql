{{ config(
    materialized = 'incremental',
    unique_key = ['a.account_id',  'o.opportunity_id', 'co.contact_id', 'cs.case_id', 'p.product_id', 'pr.pricebook_entry_id', 'pr.pricebook_id' ],
    incremental_strategy = 'merge',
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
with account_rec as 
(
    select *
     from {{ ref('vw_int_account') }}
),
case_rec as
(
    select *
    from {{ ref ('vw_int_case') }}
),
opportunity_rec as 
(
    select *
    from {{ ref ('vw_int_opportunity') }}
),
contact_rec as
(
    select * 
    from {{ ref ('vw_int_contact') }}
),
prod_rec as 
(
    select *
    from {{ ref ('vw_int_product') }}
),
price_rec as 
(
    select *
    from {{ ref ('vw_int_price_book_entry') }}
)
-- Surrogate Key
select {{ dbt_utils.generate_surrogate_key
          (['a.account_id',
           'o.opportunity_id',
           'co.contact_id',
           'cs.case_id',
           'p.product_id',
           'pr.pricebook_entry_id',
           'pr.pricebook_id'
           ]) 
          }} as acc_opp_con_case_prod_sk,
-- account

    a.account_id,
    a.parent_id,
    a.account_type,
    a.billing_street,
    a.billing_city,
    a.billing_state,
    a.billing_postal_code,
    a.billing_country,
    a.shipping_street,
    a.shipping_city,
    a.shipping_state,
    a.shipping_postal_code,
    a.shipping_country,

-- opportunity

    o.opportunity_id,
    o.opportunity_name,
    o.is_private,
    o.opportunity_description,
    o.stage_name,
    o.probability,
    o.opportunity_type,

-- contact
   co.contact_id, 
   co.first_name,
   co.last_name,
   co.salutation,
   co.mailing_street ,
   co.mailing_city,
   co.mailing_state,
   co.phone,
   co.fax,
   co.mobilephone,

-- case

   cs.case_id,
   cs.case_number,
   cs.supplied_name,
   cs.supplied_phone,
   cs.supplied_email,
   cs.supplied_company,
   cs.case_type,
   cs.case_status,
   cs.case_reason,
   cs.case_subject,

-- Prod

   p.product_id,
   p.product_code,
   p.product_type,
   p.product_class,
   p.product_description,

--Price

   pr.pricebook_entry_id,
   pr.pricebook_id,

-- DWH 
   a.is_deleted as is_account_deleted,
   o.is_deleted as is_opportunity_deleted,
   co.is_deleted as is_contact_deleted,
   cs.is_deleted as is_case_deleted,
   p.is_deleted as is_prod_deleted,
   pr.is_deleted as is_price_deleted,
   p.is_archived as is_prod_archived,
   p.is_active as is_prod_active,
   pr.is_archived as is_price_archived,
   pr.is_active as is_price_active

-- Dates   

from account_rec a 
left join opportunity_rec o 
   on (o.account_id = a.account_id)
left join contact_rec co 
   on (a.account_id = co.account_id 
      and o.contact_id = co.contact_id )
left join case_rec cs
  on (cs.account_id = a.account_id
      and cs.contact_id = co.contact_id)
left join prod_rec p 
  on (p.product_id = cs.product_id)
left join price_rec pr 
  on (p.product_id = pr.product_id)

where 1=1

{% if is_incremental() %}
  and  (
        a.acc_modified_date >= (select coalesce(max(ac.acc_modified_date),'1900-01-01') from {{this}} ac
                            where ac.account_id = a.account_id
                             )
        OR 
        o.oppr_modified_date >= (select coalesce(max(oc.oppr_modified_date),'1900-01-01') from {{this}} oc
                            where oc.opportunity_id = a.opportunity_id
                            )
        OR 
        co.con_modified_date >= (select coalesce(max(con.con_modified_date),'1900-01-01') from {{this}} con
                            where co.contact_id = con.contact_id
                            )
        OR
        cs.case_modified_date >= (select coalesce(max(csn.case_modified_date),'1900-01-01') from {{this}} csn
                            where cs.case_id = csn.case_id
                            )
        OR
        p.prod_modified_date >= (select coalesce(max(prod.prod_modified_date),'1900-01-01') from {{this}} prod
                            where p.product_id = prod.product_id
                            )

        OR
        pr.price_modified_date >= (select coalesce(max(pri.price_modified_date),'1900-01-01') from {{this}} pri
                            where pr.pricebook_entry_id = pri.pricebook_entry_id
                            )
  )

{% endif %}




 

