SELECT
  CAST(store_id AS INT64) AS store_id,
  CAST(store_name AS STRING) AS store_name,
  CAST(phone AS STRING) AS phone,
  CAST(email AS STRING) AS email,
  CAST(street AS STRING) AS street,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(zip_code AS STRING) AS zip_code
FROM {{ source('sales', 'raw_stores') }}
