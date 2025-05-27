WITH prep AS (
  SELECT
    ProductId AS product_id
    , ProductName AS product_name
    , ProductCategory AS product_category
    , price
  FROM {{source('coffee_shop_raw', 'products')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key
  , product_id
  , product_name
  , product_category
  , price
FROM prep