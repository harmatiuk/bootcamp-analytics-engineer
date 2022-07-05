with source as (

    select * from {{ from( 'inventory_transaction_types') }}
)

select
    id,
    type_name
from source