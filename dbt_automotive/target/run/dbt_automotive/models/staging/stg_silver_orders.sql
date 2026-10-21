
  
    
        create or replace table `sales_transformations`.`dbt_dev`.`stg_silver_orders`
      
      
    using delta
  
      
      
      
      
      
      
      
      as
      select
    cast(order_id as bigint) as order_id,
    cast(dealer_id as bigint) as dealer_id,
    cast(customer_id as bigint) as customer_id,
    order_number,
    booking_id,
    cast(order_date as date) as order_date,
    trim(order_status) as order_status,
    sales_executive_id,
    discount_id,
    total_amount,
    discount_amount,
    tax_amount,
    net_amount,
    payment_mode,
    finance_provider,
    created_at,
    review_required,
    quality_status,
    rejection_reason,
    review_reason
from `sales_transformations`.`default`.`silver_orders`
  