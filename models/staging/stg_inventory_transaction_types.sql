with source as (

    select * from {{ source('northwind', 'inventory_transaction_types') }}
)

select
    id,
    type_name
from source