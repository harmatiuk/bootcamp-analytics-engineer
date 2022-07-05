with source as (

    select * from {{  from('strings')  }}
)

select

    string_id,
    string_data,
    current_timestamp() as insertion_timestamp

from source