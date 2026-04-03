SELECT
  CAST(order_id AS INT64) AS order_id,
  CAST(item_id AS INT64) AS item_id,
  CAST(product_id AS INT64) AS product_id,
  CAST(quantity AS INT64) AS quantity,
  CAST(list_price AS FLOAT64) AS list_price,
  CAST(discount AS FLOAT64) AS discount
FROM {{ source('sales', 'raw_order_items') }}
