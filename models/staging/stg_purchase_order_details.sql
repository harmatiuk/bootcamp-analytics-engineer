with source as (

    select * from {{ from('purchase_order_details') }}
)


select

    id,
    purchase_order_id,
    product_id,
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory,
    inventory_id,
    current_timestamp() as insertion_timestamp

from source