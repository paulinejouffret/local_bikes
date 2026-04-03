WITH source AS (
    SELECT * FROM {{ ref('int_revenue_per_order_item') }}
)

SELECT
    DATE_TRUNC(order_date, MONTH) AS order_month,
    store_id,
    store_name,
    SUM(revenue) AS total_revenue
FROM source
GROUP BY 1, 2, 3
ORDER BY 1, 2
