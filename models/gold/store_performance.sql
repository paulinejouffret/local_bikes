WITH source AS (
    SELECT * FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
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
    s.store_id,
    s.store_name,
    EXTRACT(YEAR FROM s.order_date)                                                 AS year,
    COUNT(DISTINCT s.customer_id)                                                   AS nb_customers,
    COUNT(DISTINCT s.order_id)                                                      AS nb_orders,
    ROUND(SUM(s.revenue) / COUNT(DISTINCT s.order_id), 2)                           AS avg_basket,
    a.nb_active_staff,
    ROUND(SUM(s.revenue) / a.nb_active_staff, 2)                                    AS revenue_per_staff,
    ROUND(COUNT(DISTINCT s.order_id) / COUNT(DISTINCT s.customer_id), 2)            AS nb_orders_per_customer,
    ROUND(COUNT(DISTINCT s.customer_id) / a.nb_active_staff, 2)                     AS nb_customers_per_staff
FROM source s
LEFT JOIN active_staff_per_store a ON s.store_id = a.store_id
GROUP BY 1, 2, 3, a.nb_active_staff
ORDER BY 1, 3
