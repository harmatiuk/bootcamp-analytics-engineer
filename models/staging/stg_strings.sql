with source as (

    select * from {{  from('strings')  }}
)

select
    string_id,
    string_data
from source