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
FROM {{ ref('fact_order_details') }} AS fact_order_details
LEFT JOIN {{ ref('dim_customer') }} AS dim_customer
    ON fact_order_details.customer_key = dim_customer.customer_key
LEFT JOIN {{ ref('dim_order') }} AS dim_order
    ON fact_order_details.order_key = dim_order.order_key
LEFT JOIN {{ ref('dim_product') }} AS dim_product
    ON fact_order_details.product_key = dim_product.product_key
LEFT JOIN {{ ref('dim_store') }} AS dim_store
    ON fact_order_details.store_key = dim_store.store_key