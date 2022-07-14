with source_fact_purchase_order as (
    select * from {{ ref('fact_purchase_order') }}
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
        customer.company         as customer_company,
        customer.last_name       as customer_last_name,
        customer.first_name      as customer_first_name,
        customer.email_address   as customer_email_address,
        customer.job_title       as customer_job_title,
        customer.business_phone  as customer_business_phone,
        customer.home_phone      as customer_home_phone,
        customer.mobile_phone    as customer_mobile_phone,
        customer.fax_number      as customer,
        customer.address         as customer_address,
        customer.city            as customer_city,
        customer.state_province  as customer_state_province,
        customer.zip_postal_code as customer_zip_postal_code,
        customer.country_region  as customer_country_region,
        customer.web_page        as customer_web_page,
        customer.notes           as customer_notes,
        customer.attachments     as customer_attachments,
        employee.employee_id,
        employee.last_name       as employee_unique_employee_id,
        employee.company         as employee_company,
        employee.last_name       as employee_last_name,
        employee.first_name      as employee_first_name,
        employee.email_address   as employee_email_address,
        employee.job_title       as employee_job_title,
        employee.business_phone  as employee_business_phone,
        employee.home_phone      as employee_home_phone,
        employee.mobile_phone    as employee_mobile_phone,
        employee.fax_number      as employee_fax_number,
        employee.address         as employee_address,
        employee.city            as employee_city,
        employee.state_province  as employee_state_province,
        employee.zip_postal_code as employee_zip_postal_code,
        employee.country_region  as employee_country_region,
        employee.web_page        as employee_web_page,
        employee.notes           as employee_notes,
        employee.attachments     as employee_attachments,
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
        purchase.purchase_order_id,
        purchase.quantity,
        purchase.unit_cost,
        purchase.date_received,
        purchase.posted_to_inventory,
        purchase.inventory_id,
        purchase.supplier_id,
        purchase.created_by,
        purchase.submitted_date,
        purchase.creation_date,
        purchase.status_id,
        purchase.expected_date,
        purchase.shipping_fee,
        purchase.taxes,
        purchase.payment_date,
        purchase.payment_amount,
        purchase.payment_method,
        purchase.notes,
        purchase.approved_by,
        purchase.approved_date,
        purchase.submitted_by,
    from source_fact_purchase_order purchase
    left join source_dim_customer customer
        on purchase.customer_id = customer.customer_id
    left join source_dim_employee employee
        on purchase.employee_id = employee.employee_id
    left join source_dim_product product
        on purchase.product_id = product.product_id
)

select
*
from final