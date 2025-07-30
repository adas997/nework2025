with user_role_data as (
    select u.*
    from {{ source ('stg_source', 'users_role') }} u
)
select user_role_id,
    name as role_name,
    rollupdescription as rollup_description,
    opportunityaccessforaccountowner as opportunity_for_owner,
    caseaccessforaccountowner as case_access_for_accountowner,
    contactaccessforaccountowner as contact_access_foraccountowner,
    lastmodifieddate as modified_date,
    lastmodifiedbyid as modified_date,
    systemmodstamp as system_modstamp,
    portalaccountid as portal_accountid,
    portaltype as portal_type,
    portalrole as portal_role,
    portalaccountownerid as portal_account_ownerid
from user_role_data