WITH prep AS (
    SELECT
        StoreId AS store_id
        , StoreName AS store_name
        , district
        , city
        , address
        , latitude
        , longitude
    FROM {{source('coffee_shop_raw', 'stores')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_key
    , store_id
    , store_name
    , district
    , city
    , address
    , latitude
    , longitude
FROM prep