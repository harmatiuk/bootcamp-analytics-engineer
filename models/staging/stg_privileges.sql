with source as (

    select * from {{ from('privileges') }}
)

select
    id,
    privilege_name
from source