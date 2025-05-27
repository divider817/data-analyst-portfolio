WITH date_range AS (
    SELECT
        date
    FROM UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE())) AS date
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key
    , date
    , EXTRACT(YEAR FROM date) AS year
    , EXTRACT(QUARTER FROM date) AS quarter
    , EXTRACT(MONTH FROM date) AS month
    , EXTRACT(DAY FROM date) AS day
    , FORMAT_DATE('%A', date) AS day_name
    , CASE 
        WHEN FORMAT_DATE('%A', date) IN ('Saturday', 'Sunday') THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_range