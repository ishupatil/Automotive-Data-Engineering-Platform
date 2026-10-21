
{{ config(materialized='view') }}

select
    order_id,
    dealer_id,
    customer_id,
    order_date,
    net_amount,
    quality_status,
    review_required,
    case
        when quality_status = 'valid'
            and coalesce(review_required, false) = false
        then true
        else false
    end as is_approved_for_sales_reporting
from {{ ref('stg_silver_orders') }}
