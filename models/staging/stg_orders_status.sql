with source as (

    select * from {{ from('orders_status') }}
)

select
    id,
    status_name
from source