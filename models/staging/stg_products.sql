with source as (

    select * from {{ from('products') }}

)


select

    supplier_ids,
    id,
    product_code,
    product_name,
    description,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    attachments,
    current_timestamp() as insertion_timestamp

from source