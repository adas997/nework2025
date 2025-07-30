with case_data as (
    select c.* --,row_number () over( partition by c.case_id order by c.case_id) rn
    from {{ source ('stg_source', 'cases') }} c
)
select case_id,
    casenumber as case_number,
    isdeleted as is_deleted,
    contactid as contact_id,
    accountid as account_id,
    assetid as asset_id,
    productid as product_id,
    entitlementid as entitlement_id,
    sourceid as source_id,
    businesshoursid as businesshours_id,
    parentid as parent_id,
    suppliedname as supplied_name,
    suppliedemail as supplied_email,
    suppliedphone as supplied_phone,
    suppliedcompany as supplied_company,
    type as case_type,
    status as case_status,
    reason as case_reason,
    origin as case_origin,
    subject as case_subject,
    priority,
    description as case_description,
    stopstartdate as stop_start_date,
    createddate as case_created_date,
    createdbyid as case_created_by,
    lastmodifieddate as case_modified_date,
    lastmodifiedbyid as case_modified_by
from case_data