WITH yearly AS (
    SELECT
        EXTRACT(YEAR FROM order_date)   AS year,
        ROUND(SUM(revenue), 1)          AS total_revenue
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1
)

SELECT
    year,
    total_revenue,
    ROUND((total_revenue - LAG(total_revenue) OVER (ORDER BY year)) / LAG(total_revenue) OVER (ORDER BY year), 1) AS revenue_evol_pct
FROM yearly
ORDER BY year
