with campaign_data as (
    select c.* --row_number () over( partition by c.campaign_id, c.name order by c.name) rn
    from {{ source ('stg_source', 'campaign') }} c
)
select campaign_id,
    name as campaign_name,
    isdeleted as is_deleted,
    isactive as is_active,
    parentid as parent_id,
    type as campaign_type,
    status as campaign_status,
    startdate as campaign_start_date,
    enddate as campaign_end_date,
    expectedrevenue as campaign_revenue,
    budgetedcost as campaign_budget,
    actualcost as campaign_actual_cost,
    description as campaign_description,
    ownerid as campaign_owner,
    createddate as cmp_created_date,
    createdbyid as cmp_created_by,
    lastmodifieddate as cmp_modified_date,
    lastmodifiedbyid as cmp_modified_by
from campaign_data
where 1 = 1 --and rn=1
    --and isdeleted = 'N'