with source as (

    select * from {{ from('orders_status') }}
)

select

    id,
    status_name,
    current_timestamp() as insertion_timestamp

from source