select *
from {{ ref('int_order_metrics') }}
where is_approved_for_sales_reporting = true
  and (order_id is null or order_id <= 0)
