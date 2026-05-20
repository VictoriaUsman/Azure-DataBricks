{{
    config(materialized = 'table')
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY store_id) AS store_key,
    store_id,
    store_name,
    city,
    region,
    country
FROM {{ source('retail_platform', 'silver_stores') }}
