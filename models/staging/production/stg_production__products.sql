SELECT
  CAST(product_id AS INT64) AS product_id,
  CAST(product_name AS STRING) AS product_name,
  CAST(brand_id AS INT64) AS brand_id,
  CAST(category_id AS INT64) AS category_id,
  CAST(model_year AS INT64) AS model_year,
  CAST(list_price AS FLOAT64) AS list_price
FROM {{ source('production', 'raw_products') }}
