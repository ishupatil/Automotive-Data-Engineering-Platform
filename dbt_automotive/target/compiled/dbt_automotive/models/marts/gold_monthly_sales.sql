

select
    date_trunc('MONTH', order_date) as sales_month,
    count(*) as order_count,
    sum(net_amount) as total_net_sales,
    avg(net_amount) as average_net_order_value
from `sales_transformations`.`dbt_dev`.`int_order_metrics`
where is_approved_for_sales_reporting = true
  and order_date is not null
group by date_trunc('MONTH', order_date)