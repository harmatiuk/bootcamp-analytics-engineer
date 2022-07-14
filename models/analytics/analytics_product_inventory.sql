with source_fact_inventory as (
    select * from {{ ref('fact_inventory') }}
),
source_dim_product as (
    select * from {{ ref('dim_products') }}
),

final as (

    select
        product.product_id,
        product.product_code,
        product.product_name,
        product.description,
        product.supplier_company,
        product.standard_cost,
        product.list_price,
        product.reorder_level,
        product.target_level,
        product.quantity_per_unit,
        product.discontinued,
        product.minimum_reorder_quantity,
        product.category,
        invetory.inventory_id,
        invetory.transaction_type,
        invetory.transaction_created_date,
        invetory.transaction_modified_date,
        invetory.product_id as ipd,
        invetory.quantity,
        invetory.purchase_order_id,
        invetory.customer_order_id,
        invetory.comments,
        current_timestamp() as insertion_timestamp

    from source_fact_inventory invetory
    left join source_dim_product product
            on invetory.product_id = product.product_id
)

select * from final