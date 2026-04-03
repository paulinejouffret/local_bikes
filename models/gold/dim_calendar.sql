{{ config(materialized='table') }}

{# Daily calendar from 2019-01-01 to December 31 of current year + 1 #}

WITH digits AS (
    SELECT n FROM UNNEST([0,1,2,3,4,5,6,7,8,9]) AS n
),

numbers AS (
    SELECT a.n * 1000 + b.n * 100 + c.n * 10 + d.n AS n
    FROM digits a
    CROSS JOIN digits b
    CROSS JOIN digits c
    CROSS JOIN digits d
    WHERE a.n <= 3
),

date_spine AS (
    SELECT DATE_ADD(DATE '2016-01-01', INTERVAL n DAY) AS date_day
    FROM numbers
    WHERE DATE_ADD(DATE '2016-01-01', INTERVAL n DAY) <= DATE '2018-12-31'
)

SELECT
    date_day                                                                                                AS date,
    EXTRACT(YEAR FROM date_day)                                                                             AS year,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING), ' ', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS quarter_year,
    CAST(EXTRACT(YEAR FROM date_day) * 100 + EXTRACT(QUARTER FROM date_day) AS INT64)                      AS quarter_year_index,
    FORMAT_DATE('%B %Y', date_day)                                                                          AS month_year,
    CAST(EXTRACT(YEAR FROM date_day) * 100 + EXTRACT(MONTH FROM date_day) AS INT64)                        AS month_year_index
FROM date_spine
