WITH prep AS (
    SELECT
        customerid AS customer_id
        , registrationdate AS registration_date
        , levelofdiscount AS level_of_discount
    FROM {{source('coffee_shop_raw', 'customers')}}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} AS customer_key
    , customer_id
    , registration_date
    , level_of_discount
FROM prep