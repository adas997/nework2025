with price_date as (
    select p.*
    from {{ source ('stg_source', 'pricebook') }} p
)
select pricebook_entry_id,
    pricebook2id as pricebook_id,
    product2id product_id,
    unitprice unit_price,
    isactive as is_active,
    usestandardprice use_standard_price,
    createddate as price_created_date,
    createdbyid as price_created_by,
    lastmodifieddate as price_modified_date,
    lastmodifiedbyid as price_modified_by,
    isdeleted as is_deleted,
    isarchived as is_archived
from price_date