with source_invetory_transaction as (

    select * from {{ ref('stg_inventory_transaction_types') }}
)

select * from source_invetory_transaction