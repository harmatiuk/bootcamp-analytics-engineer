with source as (

    select
        id as customer_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        insertion_timestamp
    from {{ ref('stg_customer') }}

),

remove_duplicates as (

    select
        *,
        row_number() over(partition by customer_id) as row_number
    from source
)

select
    *
    except (row_number),
from remove_duplicates
where row_number = 1