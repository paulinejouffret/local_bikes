SELECT
  CAST(staff_id AS INT64) AS staff_id,
  CAST(first_name AS STRING) AS first_name,
  CAST(last_name AS STRING) AS last_name,
  CAST(email AS STRING) AS email,
  CAST(phone AS STRING) AS phone,
  CAST(active AS INT64) AS active,
  CAST(store_id AS INT64) AS store_id,
  CAST(manager_id AS INT64) AS manager_id
FROM {{ source('sales', 'raw_staffs') }}
