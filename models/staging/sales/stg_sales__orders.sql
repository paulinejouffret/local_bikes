SELECT
  CAST(order_id AS INT64) AS order_id,
  CAST(customer_id AS INT64) AS customer_id,
  CAST(order_status AS INT64) AS order_status,
  CAST(order_date AS DATE) AS order_date,
  CAST(required_date AS DATE) AS required_date,
  CAST(shipped_date AS DATE) AS shipped_date,
  CAST(store_id AS INT64) AS store_id,
  CAST(staff_id AS INT64) AS staff_id
FROM {{ source('sales', 'raw_orders') }}
