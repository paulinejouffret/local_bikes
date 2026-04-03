WITH source AS (
    SELECT
        store_id,
        store_name,
        ROUND(SUM(revenue), 1)          AS total_revenue,
        COUNT(DISTINCT customer_id)     AS nb_customers
    FROM {{ ref('int_revenue_per_order_item') }}
    WHERE EXTRACT(YEAR FROM order_date) IN (2016, 2017)
    GROUP BY 1, 2
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
    s.nb_customers,
    ROUND(s.total_revenue / a.nb_active_staff, 1) AS revenue_per_staff
FROM source s
LEFT JOIN active_staff_per_store a ON s.store_id = a.store_id
ORDER BY s.store_id
