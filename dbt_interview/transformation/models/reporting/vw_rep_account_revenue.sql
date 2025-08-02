with dim_details as
(
    select a.account_id as "Account Id",
    o.opportunity_id as "Opportunity Id",
    a.account_type as "Account Type",
    a.billing_street as "Billing Street",
    a.billing_city as "Billing City",
    a.billing_state as "Billing State",
    a.billing_postal_code as "Billing postal Code",
    a.billing_country as "Billing country",
    a.shipping_street as "Shipping Street",
    a.shipping_city as "Shipping City",
    a.shipping_state as "Shipping State",
    a.shipping_postal_code as "Shipping Postal Code",
    a.shipping_country as "Shipping Country",
    concat(a.first_name, ' ', a.last_name) as "Contact Full Name",
    a.mailing_street as "Contact Mailing Street",
    a.mailing_city as "Contact Mailing City",
    a.mailing_state as "Contact Mailing State",
    o.opportunity_name as "Opportunity Name",
    o.opportunity_description as "Opportunity Description",
    o.stage_name as "Opportunity Stage",
    o.opportunity_type as "Opportunity Type"
    from {{ ref ('dim_mart_accounts') }} a
        left join {{ ref ('dim_mart_opportunities')}} o
            on (a.account_id = o.account_id)
where a.is_account_deleted = 1
    and a.is_contact_deleted = 1
    and o.is_opportunity_deleted = 1
),
fct_data as
(
    select 
      f.account_id,
      f.opportunity_id,
      f.total_revenue_earned_usd as "Total Revenue earned in USD" ,
      f.total_opportunity_amount_usd as "Total Amount in USD",
      f.total_revenue_expected_usd as "Total Revenue expected in USD",
      f.average_probability as "Avg. Probability"

      from {{ ref ('fct_mart_revenues') }}  f
),
final as 
(
select
 a.*,
 f."Total Revenue earned in USD",
 f."Total Amount in USD",
 f."Total Revenue expected in USD",
 f."Avg. Probability"

 from fct_data f
    inner join dim_details a on (a."Account Id" = f.account_id and 
                                a."Opportunity Id" = f.opportunity_id)

)

select *
from final