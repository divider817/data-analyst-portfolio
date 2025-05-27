WITH prep AS (
  SELECT
    OrderId AS order_id
    , OrderType AS order_type
    , DiscountApplied AS discount_applied
  FROM {{source('coffee_shop_raw', 'orders')}}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key
  , order_id
  , order_type
  , discount_applied
FROM prep