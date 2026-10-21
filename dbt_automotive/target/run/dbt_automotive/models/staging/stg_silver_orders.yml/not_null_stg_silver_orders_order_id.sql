
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from `sales_transformations`.`dbt_dev`.`stg_silver_orders`
where order_id is null



  
  
      
    ) dbt_internal_test