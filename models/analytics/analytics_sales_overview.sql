with source_fact_sales as (
    select * from {{ ref('fact_sales') }}
),
source_dim_customer as (
    select * from {{ ref('dim_customers') }}
),
source_dim_employee as (
    select * from {{ ref('dim_employees') }}
),
source_dim_product as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        customer.customer_id,
        customer.company,
        customer.last_name,
        customer.first_name,
        customer.email_address,
        customer.job_title,
        customer.business_phone,
        customer.home_phone,
        customer.mobile_phone,
        customer.fax_number,
        customer.address,
        customer.city,
        customer.state_province,
        customer.zip_postal_code,
        customer.country_region,
        customer.web_page,
        customer.notes,
        customer.attachments,
        employee.employee_id,
        employee.company,
        employee.last_name,
        employee.first_name,
        employee.email_address,
        employee.job_title,
        employee.business_phone,
        employee.home_phone,
        employee.mobile_phone,
        employee.fax_number,
        employee.address,
        employee.city,
        employee.state_province,
        employee.zip_postal_code,
        employee.country_region,
        employee.web_page,
        employee.notes,
        employee.attachments,
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
        sales.order_id,
        sales.shipper_id,
        sales.quantity,
        sales.unit_price,
        sales.discount,
        sales.status_id,
        sales.date_allocated,
        sales.purchase_order_id,
        sales.inventory_id,
        sales.order_date,
        sales.shipped_date,
        sales.paid_date,
        current_timestamp() as insertion_timestamp
    from source_fact_sales sales
    left join source_dim_customer customer
    on customer.customer_id = sales.customer_id
    left join source_dim_employee employee
    on  sales.employee_id = employee.employee_id
    left join source_dim_product product
    on sales.product_id = product.product_id
)
select
*
from final
