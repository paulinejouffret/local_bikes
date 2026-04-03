WITH revenue_by_store_year AS (
    SELECT
        store_id,
        store_name,
        EXTRACT(YEAR FROM order_date)   AS year,
        ROUND(SUM(revenue), 2)          AS total_revenue
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1, 2, 3
)

SELECT
    store_id,
    store_name,
    year,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year)                                                                               AS prev_year_revenue,
    ROUND(total_revenue - LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year), 2)                                                     AS revenue_evolution,
    ROUND((total_revenue - LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year)) / LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY year) * 100, 2) AS revenue_evolution_pct
FROM revenue_by_store_year
ORDER BY store_id, year
