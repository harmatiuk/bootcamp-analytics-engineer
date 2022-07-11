with source as (

    select * from {{  from('inventory_transactions')  }}
)

select 
*,
current_timestamp() as insertion_timestamp

from source 