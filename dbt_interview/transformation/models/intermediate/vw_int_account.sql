with account_data
as
(
    select a.* ,
    row_number () over( partition by a.account_id, a.name order by a.name) rn
    from {{ source ('stg_source','account') }} a
)
select 
        account_id,
        isdeleted as is_deleted,
        name as account_name ,
        type as account_type ,
        parentid as parent_id,
        billingstreet as billing_street,
        billingcity as billing_city ,
        billingstate as billing_state ,
        billingpostalcode as billing_postal_code,
        billingcountry as  billing_country,
        shippingstreet as shipping_street,
        shippingcity as shipping_city ,
        shippingstate as shipping_state ,
        shippingpostalcode as shipping_postal_code,
        shippingcountry as shipping_country,
        phone,
        fax,
        accountnumber as account_number,
        industry,
        annualrevenue as annual_revenue,
        numberofemployees as number_employees,
        createddate as created_date,
        createdbyid as created_by,
        lastmodifieddate as modified_date,
        lastmodifiedbyid as modified_by
from account_data
where rn = 1