with source as (

    select * from {{ from('orders_tax_status') }}
)

select
    id,
    tax_status_name,
from source