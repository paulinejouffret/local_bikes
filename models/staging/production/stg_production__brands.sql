SELECT
  CAST(brand_id AS INT64) AS brand_id,
  CAST(brand_name AS STRING) AS brand_name
FROM {{ source('production', 'raw_brands') }}
