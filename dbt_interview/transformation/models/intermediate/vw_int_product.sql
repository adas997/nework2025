with price_date as (
    select p.*
    from {{ source ('stg_source', 'product') }} p
)
select product_id,
    name,
    productcode as product_code,
    type as product_type,
    productclass as product_class,
    description as product_description,
    isactive as is_active,
    createddate as prod_created_date,
    createdbyid as prod_created_by,
    lastmodifieddate as prod_modified_date,
    lastmodifiedbyid as prod_modified_by,
    externaldatasourceid as external_data_sourceid,
    externalid as external_id,
    quantityunitofmeasure as quantity_unit,
    isdeleted as is_deleted,
    isarchived as is_archived,
    stockkeepingunit as stock_keeping_unit
from price_date