with source as (

    select * from {{from('purchase_order_status')}}
)

select 

    id,
    status,
    current_timestamp() as insertion_timestamp

from source