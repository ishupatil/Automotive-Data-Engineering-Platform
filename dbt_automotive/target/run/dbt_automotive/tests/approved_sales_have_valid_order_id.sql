
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `sales_transformations`.`dbt_dev`.`int_order_metrics`
where is_approved_for_sales_reporting = true
  and (order_id is null or order_id <= 0)
  
  
      
    ) dbt_internal_test