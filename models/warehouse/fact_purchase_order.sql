with source_customer as (
    
    select * from {{ ref('stg_customer') }}
),

source_employees as (

    select * from {{ ref('stg_employees') }}
),

source_purchase_order_details as (

    select * from {{ ref('stg_purchase_order_details') }}
),

source_products as (

    select * from {{ ref('stg_products') }}
),

source_purchase_order as (

    select * from {{ ref('stg_purchase_orders') }}
),

source_order_details as (

    select * from {{ ref('stg_order_details') }} 
),

source_orders as (

    select * from {{ ref('stg_orders') }}
),

purchase_order as (


    select
        customer.id as customer_id,
        employees.id as employee_id,
        purchase_order_details.purchase_order_id,
        purchase_order_details.product_id,
        purchase_order_details.quantity,
        purchase_order_details.unit_cost,
        purchase_order_details.date_received,
        purchase_order_details.posted_to_inventory,
        purchase_order_details.inventory_id,
        purchase_order.supplier_id,
        purchase_order.created_by,
        purchase_order.submitted_date,
        purchase_order.creation_date,
        purchase_order.status_id,
        purchase_order.expected_date,
        purchase_order.shipping_fee,
        purchase_order.taxes,
        purchase_order.payment_date,
        purchase_order.payment_amount,
        purchase_order.payment_method,
        purchase_order.notes,
        purchase_order.approved_by,
        purchase_order.approved_date,
        purchase_order.submitted_by,
        current_timestamp() as insertion_timestamp
    from source_purchase_order purchase_order
    left join source_purchase_order_details purchase_order_details
        on purchase_order.id = purchase_order_details.purchase_order_id
    left join source_products products
        on  purchase_order_details.product_id = products.id
    left join  source_order_details order_details
        on products.id = order_details.product_id
    left join source_orders orders
        on order_details.order_id = orders.id
    left join source_employees employees
        on purchase_order.created_by = employees.id
    left join source_customer customer
        on orders.customer_id = customer.id
    where orders.customer_id is not null
),

remove_duplicates as (

    select
        *,
        row_number() over(partition by customer_id, employee_id, purchase_order_id, product_id, inventory_id, supplier_id, creation_date) as row_number
    from purchase_order

)

select
*
except (row_number),
from remove_duplicates
where row_number = 1


