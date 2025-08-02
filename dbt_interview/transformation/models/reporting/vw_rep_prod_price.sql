with case_details as
(
    select c.case_id as "Case Id",
    c.account_id as "Account Id",
    c.contact_id as "Contact Id",
    c.product_id as "Product Id",
    c.supplied_name as "Case Supplied Name",
    c.supplied_email as "Case Supplied Email",
    c.supplied_phone as "Case Supplied Phone" ,
    c.supplied_company as "Case Supplied Company",
    c.case_type as "Case Type",
    c.case_status as "Case Status" ,
    c.case_subject as "Case Subject",
    c.priority as "Case Priority",
    c.case_description as "Case Description",
    c.stop_start_date as "Case Start Date",
    p.product_code as "Product Code",
    p.product_type as "Product Type" ,
    p.product_class as "Product Class",
    p.product_description as "Product Description",
    p.quantity_unit as "Product Quantity Unit",
    a.account_type as "Account Type"
    from {{ ref ('dim_mart_products') }} p
    join {{ ref ('dim_mart_cases') }} c on (p.product_id = c.product_id)
    join {{ ref('dim_mart_accounts') }} a on (  c.account_id = a.account_id 
                                            and c.contact_id = a.contact_id )
where a.is_account_deleted = 1
    and a.is_contact_deleted = 1
    and p.is_prod_deleted = 1
    and c.is_deleted = 1
)
,
final as (
   select c.*,
    f.min_unit_price_usd as "Min Unit Price (USD)",
    f.max_unit_price_usd as "Max Unit Price (USD)",
    f.min_use_standard_price_usd as "Min Standard Price (USD)",
    f.max_use_standard_price_usd as "Max Standard Price (USD)",
    f.total_unit_price_usd as "Total Unit Price (USD)"
from {{ ref ('fct_mart_prod_price') }} f
    join case_details c on 
    (
        c."Case Id" = f.case_id 
        and c."Product Id" = f.product_id
        and c."Account Id" = f.account_id
    )
)
select * from final