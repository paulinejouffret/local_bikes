SELECT
  CAST(category_id AS INT64) AS category_id,
  CAST(category_name AS STRING) AS category_name
FROM {{ source('production', 'raw_categories') }}
