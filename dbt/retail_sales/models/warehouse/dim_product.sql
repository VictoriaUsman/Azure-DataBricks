{{
    config(materialized = 'table')
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.unit_price,
    COALESCE(perf.return_rate,   0.0) AS return_rate,
    COALESCE(perf.unique_buyers, 0)   AS unique_buyers,
    COALESCE(perf.category_rank, 999) AS category_rank
FROM {{ source('retail_platform', 'silver_products') }} p
LEFT JOIN {{ ref('gold_product_performance') }} perf USING (product_id)
