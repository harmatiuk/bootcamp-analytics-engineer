with source as (

    select
        id as product_id,
        product_code,
        product_name,
        description,
        supplier_ids as supplier_company,
        standard_cost,
        list_price,
        reorder_level,
        target_level,
        quantity_per_unit,
        discontinued,
        minimum_reorder_quantity,
        category,
        attachments,
        insertion_timestamp
    from {{ ref('stg_products') }}

)

select
    *
from source