{{ config(materialized='table', file_format='delta') }}

select
    coalesce(quality_status, 'Unknown') as quality_status,
    coalesce(review_required, false) as review_required,
    count(*) as order_count,
    sum(case when order_id is null then 1 else 0 end) as missing_order_id_count
from {{ ref('stg_silver_orders') }}
group by
    coalesce(quality_status, 'Unknown'),
    coalesce(review_required, false)
