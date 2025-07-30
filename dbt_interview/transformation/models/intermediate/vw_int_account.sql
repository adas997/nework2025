with account_data as (
    select a.* -- ,row_number () over( partition by a.account_id, a.name order by a.name) rn
    from {{ source ('stg_source', 'account') }} a
)
select account_id,
    name as account_name,
    type as account_type,
    isdeleted as is_deleted,
    parentid as parent_id,
    billingstreet as billing_street,
    billingcity as billing_city,
    billingstate as billing_state,
    billingpostalcode as billing_postal_code,
    billingcountry as billing_country,
    shippingstreet as shipping_street,
    shippingcity as shipping_city,
    shippingstate as shipping_state,
    shippingpostalcode as shipping_postal_code,
    shippingcountry as shipping_country,
    phone,
    fax,
    accountnumber as account_number,
    industry,
    annualrevenue as annual_revenue,
    numberofemployees as number_employees,
    createddate as acc_created_date,
    createdbyid as acc_created_by,
    lastmodifieddate as acc_modified_date,
    lastmodifiedbyid as acc_modified_by
from account_data
where 1 = 1 --and rn = 1
    --and isdeleted = 'N'