with contact_data as (
    select c.* --, row_number () over( partition by c.contact_id order by c.contact_id) rn
    from {{ source ('stg_source', 'contact') }} c
)
select contact_id,
    accountid as account_id,
    isdeleted as is_deleted,
    salutation,
    firstname as first_name,
    lastname as last_name,
    otherstreet as other_street,
    othercity as other_city,
    otherstate as other_state,
    othercountry as other_country,
    otherpostalcode as other_postal_code,
    mailingstreet as mailing_street,
    mailingcity as mailing_city,
    mailingstate as mailing_state,
    mailingpostalcode as mailing_postal_code,
    mailingcountry as mailing_country,
    phone,
    fax,
    mobilephone,
    homephone,
    otherphone,
    assistantphone,
    reportstoid,
    email,
    title,
    createddate as con_created_date,
    createdbyid as con_created_by,
    lastmodifieddate as con_modified_date,
    lastmodifiedbyid as con_modified_by
from contact_data
where 1 = 1