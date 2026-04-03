WITH source AS (
    SELECT * FROM {{ ref('int_revenue_per_order_item') }}
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
    COUNT(DISTINCT s.customer_id)                           AS nb_customers,
    COUNT(DISTINCT s.order_id)                              AS nb_orders,
    ROUND(SUM(s.revenue) / COUNT(DISTINCT s.order_id), 2)   AS avg_basket,
    a.nb_active_staff,
    ROUND(SUM(s.revenue) / a.nb_active_staff, 2)            AS revenue_per_staff
FROM source s
LEFT JOIN active_staff_per_store a ON s.store_id = a.store_id
GROUP BY 1, 2, a.nb_active_staff
ORDER BY 1
