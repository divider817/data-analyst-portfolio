WITH order_categories AS (
    SELECT
        od.order_key
        , MAX(CASE WHEN p.product_category = 'Beverage' THEN 1 ELSE 0 END) AS has_beverage
        , MAX(CASE WHEN p.product_category = 'Pastry' THEN 1 ELSE 0 END) AS has_pastry
        , MAX(CASE WHEN p.product_category = 'Savory' THEN 1 ELSE 0 END) AS has_savory
    FROM {{ ref('fact_order_details') }} AS od
    LEFT JOIN {{ ref('dim_product') }} AS p
        ON od.product_key = p.product_key
    GROUP BY od.order_key
),

categorized_orders AS (
    SELECT
        order_key
        , CASE
            WHEN has_beverage + has_pastry + has_savory = 3 THEN 'All Categories'
            WHEN has_beverage = 1 AND has_pastry = 1 AND has_savory = 0 THEN 'Beverage + Pastry'
            WHEN has_beverage = 1 AND has_savory = 1 AND has_pastry = 0 THEN 'Beverage + Savory'
            WHEN has_pastry = 1 AND has_savory = 1 AND has_beverage = 0 THEN 'Pastry + Savory'
            WHEN has_beverage = 1 AND has_pastry = 0 AND has_savory = 0 THEN 'Beverage Only'
            WHEN has_pastry = 1 AND has_beverage = 0 AND has_savory = 0 THEN 'Pastry Only'
            WHEN has_savory = 1 AND has_beverage = 0 AND has_pastry = 0 THEN 'Savory Only'
            ELSE 'Other'
        END AS product_mix_type
    FROM order_categories
)

SELECT
    fact_order_details.order_detail_id
    , fact_order_details.order_date
    , dim_customer.registration_date
    , dim_customer.level_of_discount
    , dim_order.order_type
    , dim_order.order_id
    , dim_order.discount_applied
    , dim_product.product_name
    , dim_product.product_category
    , dim_product.price
    , dim_store.store_name
    , dim_store.latitude
    , dim_store.longitude
    , dim_store.address
    , dim_store.district
    , fact_order_details.quantity
    , fact_order_details.sub_total
    , fact_order_details.discount_rate
    , fact_order_details.discount_amount
    , ROUND(fact_order_details.total_amount, 2) AS total_amount
    , categorized_orders.product_mix_type
FROM {{ ref('fact_order_details') }} AS fact_order_details
LEFT JOIN {{ ref('dim_customer') }} AS dim_customer
    ON fact_order_details.customer_key = dim_customer.customer_key
LEFT JOIN {{ ref('dim_order') }} AS dim_order
    ON fact_order_details.order_key = dim_order.order_key
LEFT JOIN {{ ref('dim_product') }} AS dim_product
    ON fact_order_details.product_key = dim_product.product_key
LEFT JOIN {{ ref('dim_store') }} AS dim_store
    ON fact_order_details.store_key = dim_store.store_key
LEFT JOIN categorized_orders
    ON fact_order_details.order_key = categorized_orders.order_key