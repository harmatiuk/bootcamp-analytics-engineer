with source as (

    select * from {{ from('privileges') }}
)

select

    id,
    privilege_name,
    current_timestamp() as insertion_timestamp

from source