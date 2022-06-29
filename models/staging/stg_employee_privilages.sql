with source as (

    select * from {{ source('northwind', 'employee_privileges') }}
)

select
    employee_id,
    privilege_id 
from source