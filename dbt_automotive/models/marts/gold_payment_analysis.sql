{{ config(materialized='table', file_format='delta') }}

select
    coalesce(payment_mode, 'Unknown') as payment_mode,
    coalesce(finance_provider, 'Unknown') as finance_provider,
    count(*) as order_count,
    sum(net_amount) as total_net_sales
from {{ ref('int_order_metrics') }}
where is_approved_for_sales_reporting = true
group by
    coalesce(payment_mode, 'Unknown'),
    coalesce(finance_provider, 'Unknown')
