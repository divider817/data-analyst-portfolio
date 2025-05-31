WITH prep AS (
    SELECT
        order_details.OrderDetailId AS order_detail_id
        , orders.OrderId AS order_id
        , orders.CustomerId AS customer_id
        , orders.StoreId AS store_id
        , order_details.ProductId AS product_id
        , orders.OrderDate AS order_date
        , order_details.quantity
    FROM {{source('coffee_shop_raw', 'orders')}} AS orders
    INNER JOIN {{source('coffee_shop_raw', 'order_details')}} AS order_details
        ON orders.OrderId = order_details.OrderId
)

, calc AS (
    SELECT
        prep.order_detail_id AS order_detail_id
        , dim_order.order_key AS order_key
        , dim_customer.customer_key AS customer_key
        , dim_store.store_key AS store_key
        , dim_product.product_key AS product_key
        , prep.order_date
        --, dim_date.date_key
        , prep.quantity
        , prep.quantity * dim_product.price AS sub_total
        , dim_customer.level_of_discount AS discount_rate
    FROM prep
    INNER JOIN {{ ref('dim_order') }} AS dim_order
        ON prep.order_id = dim_order.order_id
    LEFT JOIN {{ ref('dim_customer') }} AS dim_customer
        ON prep.customer_id = dim_customer.customer_id
    LEFT JOIN {{ ref('dim_store') }} AS dim_store
        ON prep.store_id = dim_store.store_id
    LEFT JOIN {{ ref('dim_product') }} AS dim_product
        ON prep.product_id = dim_product.product_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_detail_id']) }} AS order_detail_key
    , order_detail_id
    , order_key
    , customer_key
    , store_key
    , product_key
    , order_date
    , quantity
    , sub_total
    , discount_rate
    , sub_total * discount_rate AS discount_amount
    , sub_total - (sub_total * discount_rate) AS total_amount
FROM calc