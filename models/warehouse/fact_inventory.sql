with source_invetory_transaction as (

    select * from {{ ref('stg_inventory_transactions') }}
)

select
    id as inventory_id,
    transaction_type,
    transaction_created_date,
    transaction_modified_date,
    product_id,
    quantity,
    purchase_order_id,
    customer_order_id,
    comments,
    insertion_timestamp
from source_invetory_transaction