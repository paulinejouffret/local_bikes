WITH source AS (
    SELECT * FROM {{ ref('int_revenue_per_order_item') }}
)

SELECT
    store_id,
    store_name,
    COUNT(DISTINCT customer_id)                         AS nb_customers,
    COUNT(DISTINCT order_id)                            AS nb_orders,
    ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2)   AS avg_basket
FROM source
GROUP BY 1, 2
ORDER BY 1
