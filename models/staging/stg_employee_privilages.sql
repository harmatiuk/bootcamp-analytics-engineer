with source as (

    select * from {{ from('employee_privileges') }}
)

select

    employee_id,
    privilege_id,
    current_timestamp() as insertion_timestamp

from source