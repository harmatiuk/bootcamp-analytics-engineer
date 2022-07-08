with source_order_details as (

    select
        *
    from {{ ref('stg_order_details') }}
),

source_orders as (

    select
        *
    from {{ ref('stg_orders') }}
),

joined as (

    select

        order_details.order_id,
        order_details.product_id,
        orders.customer_id,
        orders.employee_id,
        orders.shipper_id,
        order_details.quantity,
        order_details.unit_price,
        order_details.discount,
        order_details.status_id,
        order_details.date_allocated,
        order_details.purchase_order_id,
        order_details.inventory_id,
        orders.order_date,
        orders.shipped_date,
        orders.paid_date

    from source_orders orders
    left join source_order_details order_details
        on orders.id = order_details.order_id
    where order_details.order_id is not null

),

remove_duplicates as (

    select
        *,
        row_number() over(partition by customer_id, employee_id, order_id, product_id, shipper_id, purchase_order_id, order_date) as row_number
    from joined
    
)

select
    *
    EXCEPT(row_number),
from remove_duplicates
where row_number = 1
