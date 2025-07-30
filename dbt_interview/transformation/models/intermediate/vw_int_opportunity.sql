with opportunity_data as (
    select o.*
    from {{ source ('stg_source', 'opportunity') }} o
)
select opportunity_id,
    accountid as account_id,
    case when isprivate = 0 then 'Private' 
         else 'Public' 
         end as is_private,
    isdeleted as is_deleted,
    name as opportunity_name,
    description as opportunity_description,
    stagename as stage_name,
    stagesortorder as stage_sort_order,
    amount as amount,
    probability as probability,
    expectedrevenue as expected_revenue,
    totalopportunityquantity as total_opportunity_quantity,
    closedate as close_date,
    type as opportunity_type,
    nextstep as next_step,
    leadsource as lead_source,
    isclosed as is_closed,
    ownerid as owner_id,
    contactid as contact_id,
    createddate as oppr_created_date,
    createdbyid as oppr_created_by,
    lastmodifieddate as oppr_modified_date,
    lastmodifiedbyid as oppr_modified_by
from opportunity_data --where isdeleted = 'N'