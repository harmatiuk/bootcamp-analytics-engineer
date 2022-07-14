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
        customer.company            as customer_company,
        customer.last_name          as customer_last_name,
        customer.first_name         as customer_first_name,
        customer.email_address      as customer_email_address,
        customer.job_title          as customer_job_title,
        customer.business_phone     as customer_business_phone,
        customer.home_phone         as customer_home_phone,
        customer.mobile_phone       as customer_mobile_phone,
        customer.fax_number         as customer_fax_number,
        customer.address            as customer_address,
        customer.city               as customer_city,
        customer.state_province     as customer_state_province,
        customer.zip_postal_code    as customer_zip_postal_code,
        customer.country_region     as customer_country_region,
        customer.web_page           as customer_web_page,
        customer.notes              as customer_notes,
        customer.attachments        as customer_attachments,
        employee.employee_id,
        employee.company            as employee_company,
        employee.last_name          as employee_last_name,
        employee.first_name         as employee_first_name,
        employee.email_address      as employee_email_address,
        employee.job_title          as employee_job_title,
        employee.business_phone     as employee_business_phone,
        employee.home_phone         as employee_home_phone,
        employee.mobile_phone       as employee_mobile_phone,
        employee.fax_number         as employee_fax_number,
        employee.address            as employee_address,
        employee.city               as employee_city,
        employee.state_province     as employee_state_province,
        employee.zip_postal_code    as employee_zip_postal_code,
        employee.country_region     as employee_country_region,
        employee.web_page           as employee_web_page,
        employee.notes              as employee_notes,
        employee.attachments        as employee_attachments,
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
