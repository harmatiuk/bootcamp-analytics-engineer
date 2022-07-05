with source as (

    select * from {{ from('orders_tax_status') }}
)

select

    id,
    tax_status_name,
    current_timestamp() as insertion_timestamp

from source