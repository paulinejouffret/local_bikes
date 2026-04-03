WITH orders AS (
    SELECT * FROM {{ ref('stg_sales__orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_sales__order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_production__products') }}
),

stores AS (
    SELECT * FROM {{ ref('stg_sales__stores') }}
),

brands AS (
    SELECT * FROM {{ ref('stg_production__brands') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_production__categories') }}
)

SELECT
    -- order
    o.order_id,
    o.order_date,
    o.order_status,
    o.customer_id,
    o.staff_id,

    -- store
    o.store_id,
    s.store_name,

    -- order items
    oi.item_id,
    oi.product_id,
    oi.quantity,
    oi.list_price,
    oi.discount,

    -- product
    p.product_name,
    p.model_year,
    p.brand_id,
    p.category_id,

    -- brand
    b.brand_name,

    -- category
    c.category_name,

    -- revenue
    ROUND(oi.quantity * oi.list_price * (1 - oi.discount), 2) AS revenue

FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
LEFT JOIN stores s ON o.store_id = s.store_id
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN categories c ON p.category_id = c.category_id
