WITH prep AS (
    SELECT
        orders.OrderId
        , orders.OrderDate
        , orders.TotalAmount
        , orders.OrderType
        , order_details.ProductName
        , order_details.ProductCategory
    FROM {{source('coffee_shop_raw', 'order')}} AS orders
    INNER JOIN {{source('coffee_shop_raw', 'order_details')}} AS order_details
        ON orders.OrderId = order_details.OrderId
)

SELECT
    OrderId as order_id
    , OrderDate as order_date
    , TotalAmount as total_amount
    , OrderType as order_type
    , ProductName as product_name
    , ProductCategory as product_category
FROM prep