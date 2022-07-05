with source as (

    select * from {{ from( 'inventory_transaction_types') }}
)

select

    id,
    type_name,
    current_timestamp() as insertion_timestamp

from source