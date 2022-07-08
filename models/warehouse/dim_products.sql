with source_products as (

    select
      *
    from {{ ref('stg_products') }}

),


source_suppliers as (

    select
        *
    from {{ ref('stg_suppliers') }}
),

products as (

       select
            products.id as product_id,
            products.product_code,
            products.product_name,
            products.description,
            suppliers.company as supplier_company,
            products.standard_cost,
            products.list_price,
            products.reorder_level,
            products.target_level,
            products.quantity_per_unit,
            products.discontinued,
            products.minimum_reorder_quantity,
            products.category,
            products.attachments,
            products.insertion_timestamp
        from source_products products
        left join source_suppliers suppliers
                on products.supplier_ids = suppliers.id

),

remove_duplicates as (

    select
        *,
        row_number() over(partition by product_id) as row_number
    from products
)

select
    *
    except (row_number),
from remove_duplicates
where row_number = 1