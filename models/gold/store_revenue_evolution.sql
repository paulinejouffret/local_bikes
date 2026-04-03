WITH revenue_by_store_year AS (
    SELECT
        store_id,
        store_name,
        EXTRACT(YEAR FROM order_date)   AS year,
        SUM(revenue)                    AS total_revenue
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1, 2, 3
),

pivoted AS (
    SELECT
        store_id,
        store_name,
        SUM(CASE WHEN year = 2016 THEN total_revenue ELSE 0 END) AS revenue_2016,
        SUM(CASE WHEN year = 2017 THEN total_revenue ELSE 0 END) AS revenue_2017
    FROM revenue_by_store_year
    GROUP BY 1, 2
),

store_evolution AS (
    SELECT
        store_id,
        store_name,
        ROUND(revenue_2016, 2)                                          AS revenue_2016,
        ROUND(revenue_2017, 2)                                          AS revenue_2017,
        ROUND(revenue_2017 - revenue_2016, 2)                           AS revenue_evolution,
        ROUND((revenue_2017 - revenue_2016) / revenue_2016 * 100, 2)   AS revenue_evolution_pct
    FROM pivoted
),

global_evolution AS (
    SELECT
        CAST(NULL AS INT64)             AS store_id,
        'TOTAL'                         AS store_name,
        ROUND(SUM(revenue_2016), 2)     AS revenue_2016,
        ROUND(SUM(revenue_2017), 2)     AS revenue_2017,
        ROUND(SUM(revenue_2017) - SUM(revenue_2016), 2)                         AS revenue_evolution,
        ROUND((SUM(revenue_2017) - SUM(revenue_2016)) / SUM(revenue_2016) * 100, 2) AS revenue_evolution_pct
    FROM pivoted
)

SELECT * FROM store_evolution
UNION ALL
SELECT * FROM global_evolution
ORDER BY store_id NULLS LAST
