with source as (

    select * from {{ from('order_details') }}
)

select

    *,
    current_timestamp() as insertion_timestamp

from source