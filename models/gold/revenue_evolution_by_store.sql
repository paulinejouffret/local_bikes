WITH source AS (
    SELECT
        store_id,
        store_name,
        EXTRACT(YEAR FROM order_date)                                   AS year,
        ROUND(SUM(revenue), 1)                                          AS total_revenue,
        ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 1)               AS avg_basket
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1, 2, 3
)

SELECT
    store_id,
    store_name,
    year,
    total_revenue,
    ROUND((total_revenue - LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year)) / LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year), 1) AS revenue_evol_pct,
    avg_basket,
    ROUND((avg_basket - LAG(avg_basket) OVER (PARTITION BY store_id ORDER BY year)) / LAG(avg_basket) OVER (PARTITION BY store_id ORDER BY year), 1)         AS avg_basket_evol_pct
FROM source
ORDER BY store_id, year
