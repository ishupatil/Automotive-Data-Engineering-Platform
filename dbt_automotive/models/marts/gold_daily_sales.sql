{{ config(materialized='table', file_format='delta') }}

select
    order_date as sales_date,
    count(*) as order_count,
    count(distinct dealer_id) as dealer_count,
    sum(net_amount) as total_net_sales,
    sum(total_amount) as total_gross_amount
from {{ ref('int_order_metrics') }}
where is_approved_for_sales_reporting = true
  and order_date is not null
group by order_date
