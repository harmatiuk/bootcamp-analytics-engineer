with source as (

    select * from {{ from('products') }}

),

transform_source as (

    select

        case
            when supplier_ids = '2;6' then 2
            when supplier_ids = '3;4' then 3
            else cast(supplier_ids as INT64)
        end as supplier_ids,
        
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
)


select
*
from transform_source