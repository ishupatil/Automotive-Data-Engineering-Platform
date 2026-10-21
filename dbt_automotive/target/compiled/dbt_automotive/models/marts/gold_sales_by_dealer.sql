

select
    dealer_id,
    count(*) as order_count,
    sum(net_amount) as total_net_sales,
    avg(net_amount) as average_net_order_value
from `sales_transformations`.`dbt_dev`.`int_order_metrics`
where is_approved_for_sales_reporting = true
  and dealer_id is not null
group by dealer_id