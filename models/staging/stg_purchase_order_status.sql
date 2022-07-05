with source as (

    select * from {{from('purchase_order_status')}}
)

select 

    id,
    status

from source