WITH order_counts AS (
    SELECT 
        customer_key
        , COUNT(DISTINCT order_key) AS orders_all_time
        , COUNT(DISTINCT CASE 
            WHEN order_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -180 DAY) 
            THEN order_key 
        END) AS orders_past_180_days
        , MAX(order_date) AS last_order_date
    FROM {{ ref('fact_order_details') }}
    WHERE customer_key IS NOT NULL
    GROUP BY customer_key
)

, ltv_metrics AS (
    SELECT 
        od.customer_key
        , dc.registration_date
        , DATE_DIFF(CURRENT_DATE(), dc.registration_date, DAY) AS customer_age_days
        , ROUND(SUM(od.total_amount), 2) AS ltv
    FROM {{ ref('fact_order_details') }} od
    JOIN {{ ref('dim_customer') }} dc
        ON od.customer_key = dc.customer_key
    GROUP BY od.customer_key, dc.registration_date
)

, rfm_prep AS (
    SELECT
        customer_key
        , DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS recency
        , COUNT(DISTINCT order_key) AS frequency
        , SUM(total_amount) AS monetary
    FROM {{ ref('fact_order_details') }}
    WHERE customer_key IS NOT NULL
    GROUP BY customer_key
)

, rfm_tiles AS (
SELECT
    customer_key
    , NTILE(5) OVER (ORDER BY recency DESC) AS r_score
    , NTILE(5) OVER (ORDER BY frequency ASC) AS f_score
    , NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
FROM rfm_prep
)

, rfm_segments AS (
    SELECT
        customer_key
        , r_score
        , f_score
        , m_score
        , CASE
            WHEN r_score > 4 AND f_score > 4 AND m_score > 4 THEN 'Champions'
            WHEN r_score <= 3 AND f_score <= 3 AND m_score >= 4 THEN 'Big Spenders'
            WHEN r_score >= 3 AND f_score >= 4 AND m_score >= 3 THEN 'Loyal'
            WHEN r_score >= 4 AND f_score <= 3 THEN 'New Customers'
            WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
            WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Churned'
            ELSE 'Other'
        END AS segment
    FROM rfm_tiles
)

SELECT
    oc.customer_key
    , ltv.registration_date
    , CASE
        WHEN oc.orders_past_180_days > 0 THEN 1
        ELSE 0
    END AS is_active_180d
    , oc.orders_all_time
    , oc.orders_past_180_days
    , DATE_DIFF(CURRENT_DATE(), oc.last_order_date, DAY) AS days_since_last_order
    , ltv.ltv
    , ltv.customer_age_days
    , ROUND(ltv.ltv / NULLIF(oc.orders_all_time, 0), 2) AS avg_order_value
    , rfm.r_score
    , rfm.f_score
    , rfm.m_score
    , rfm.segment
FROM order_counts oc
LEFT JOIN ltv_metrics ltv 
    ON oc.customer_key = ltv.customer_key
LEFT JOIN rfm_segments rfm 
    ON oc.customer_key = rfm.customer_key