SELECT
  CAST(store_id AS INT64) AS store_id,
  CAST(product_id AS INT64) AS product_id,
  CAST(quantity AS INT64) AS quantity
FROM {{ source('production', 'raw_stocks') }}
