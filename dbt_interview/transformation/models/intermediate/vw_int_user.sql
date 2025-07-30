with user_data as (
    select u.*
    from {{ source ('stg_source', 'users') }} u
)
select user_id,
    contactid as contact_id,
    accountid as account_id,
    userroleid as user_role_id,
    isactive as is_active,
    username as user_name,
    firstname as first_name,
    lastname as last_name,
    companyname as company_name,
    division,
    department,
    title,
    street,
    city,
    state,
    postalcode,
    country,
    phone,
    fax,
    mobilephone,
    alias,
    createddate as user_created_date,
    createdbyid as user_created_by,
    lastmodifieddate as user_modified_date,
    lastmodifiedbyid as user_modified_by
from user_data