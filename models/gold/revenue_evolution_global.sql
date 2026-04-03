WITH yearly AS (
    SELECT
        EXTRACT(YEAR FROM order_date)                                   AS year,
        ROUND(SUM(revenue), 2)                                          AS total_revenue,
        COUNT(DISTINCT order_id)                                        AS nb_orders,
        COUNT(DISTINCT customer_id)                                     AS nb_customers,
        ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2)               AS avg_basket
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1
)

SELECT
    year,
    total_revenue,
    nb_orders,
    nb_customers,
    avg_basket,
    LAG(total_revenue)  OVER (ORDER BY year)                                                            AS prev_total_revenue,
    LAG(nb_orders)      OVER (ORDER BY year)                                                            AS prev_nb_orders,
    LAG(nb_customers)   OVER (ORDER BY year)                                                            AS prev_nb_customers,
    LAG(avg_basket)     OVER (ORDER BY year)                                                            AS prev_avg_basket,
    ROUND((total_revenue - LAG(total_revenue) OVER (ORDER BY year)) / LAG(total_revenue) OVER (ORDER BY year) * 100, 2)     AS revenue_evolution_pct,
    ROUND((nb_orders - LAG(nb_orders) OVER (ORDER BY year)) / LAG(nb_orders) OVER (ORDER BY year) * 100, 2)                 AS nb_orders_evolution_pct,
    ROUND((nb_customers - LAG(nb_customers) OVER (ORDER BY year)) / LAG(nb_customers) OVER (ORDER BY year) * 100, 2)        AS nb_customers_evolution_pct,
    ROUND((avg_basket - LAG(avg_basket) OVER (ORDER BY year)) / LAG(avg_basket) OVER (ORDER BY year) * 100, 2)              AS avg_basket_evolution_pct
FROM yearly
ORDER BY year
