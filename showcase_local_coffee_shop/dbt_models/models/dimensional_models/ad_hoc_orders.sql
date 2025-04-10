select *
from {{ ref('fact_orders') }}
where order_id in (1,2,3,4,5)
