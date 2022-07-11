with generate_date as (

    select
        * 
    from unnest(generate_date_array('2014-01-01', '2050-01-01', interval 1 day)) as date
),

format_date as (

    select
        format_date('%F', date) as id,
        date as full_date,
        extract(YEAR from date) as year,
        extract(WEEK from date) as year_week,
        extract(DAY from date) as year_day,
        extract(YEAR from date) as fiscal_year,
        format_date('%Q', date) as fiscal_qrt,
        extract(MONTH from date) as month,
        format_date('%B', date) as month_name,
        format_date('%w', date) as week_day,
        format_date('%A', date) as day_name,
        
        case
            when format_date('%A', date) in ('Sunday', 'Saturday') then 0
            else 1
        end as day_is_weekday

    from generate_date

)

select
*
from format_date
