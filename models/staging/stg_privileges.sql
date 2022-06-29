with source as (

    select * from {{ source('northwind', 'privileges') }}
)

select
    id,
    privilege_name
from source