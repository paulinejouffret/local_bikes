WITH revenue AS (
    SELECT
        store_id,
        store_name,
        EXTRACT(YEAR FROM order_date)                                   AS year,
        ROUND(SUM(revenue), 2)                                          AS total_revenue,
        COUNT(DISTINCT order_id)                                        AS nb_orders,
        COUNT(DISTINCT customer_id)                                     AS nb_customers,
        ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2)               AS avg_basket,
        ROUND(AVG(discount) * 100, 2)                                   AS avg_discount_pct
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1, 2, 3
),

active_staff_per_store AS (
    SELECT
        store_id,
        COUNT(*) AS nb_active_staff
    FROM {{ ref('stg_sales__staffs') }}
    WHERE active = 1
    GROUP BY 1
)

SELECT
    r.store_id,
    r.store_name,
    r.year,
    r.total_revenue,
    r.nb_orders,
    r.nb_customers,
    r.avg_basket,
    r.avg_discount_pct,
    a.nb_active_staff,
    ROUND(r.total_revenue / a.nb_active_staff, 2)                                                                                               AS revenue_per_staff,
    ROUND(r.nb_customers / a.nb_active_staff, 2)                                                                                                AS nb_customers_per_staff,
    LAG(r.total_revenue) OVER (PARTITION BY r.store_id ORDER BY r.year)                                                                         AS prev_total_revenue,
    ROUND((r.total_revenue - LAG(r.total_revenue) OVER (PARTITION BY r.store_id ORDER BY r.year)) / LAG(r.total_revenue) OVER (PARTITION BY r.store_id ORDER BY r.year) * 100, 2) AS revenue_evolution_pct,
    ROUND((r.avg_basket - LAG(r.avg_basket) OVER (PARTITION BY r.store_id ORDER BY r.year)) / LAG(r.avg_basket) OVER (PARTITION BY r.store_id ORDER BY r.year) * 100, 2)         AS avg_basket_evolution_pct,
    ROUND((r.nb_customers - LAG(r.nb_customers) OVER (PARTITION BY r.store_id ORDER BY r.year)) / LAG(r.nb_customers) OVER (PARTITION BY r.store_id ORDER BY r.year) * 100, 2)   AS nb_customers_evolution_pct
FROM revenue r
LEFT JOIN active_staff_per_store a ON r.store_id = a.store_id
ORDER BY r.store_id, r.year
