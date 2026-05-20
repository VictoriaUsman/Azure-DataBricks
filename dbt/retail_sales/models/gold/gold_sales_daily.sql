{{
    config(
        materialized = 'table',
        partition_by = {'field': 'transaction_date', 'data_type': 'date'},
        post_hook    = "OPTIMIZE {{ this }} ZORDER BY (store_id, category, transaction_date)"
    )
}}

SELECT
    t.transaction_date,
    t.store_id,
    s.region,
    t.product_id,
    p.category,
    p.subcategory,
    COUNT(t.transaction_id)          AS transaction_count,
    SUM(t.quantity)                  AS units_sold,
    ROUND(SUM(t.revenue_net), 2)     AS revenue,
    ROUND(AVG(t.discount), 4)        AS avg_discount_rate,
    SUM(t.is_returned)               AS return_count,
    COUNT(DISTINCT t.customer_id)    AS unique_customers
FROM {{ source('retail_platform', 'silver_transactions') }} t
LEFT JOIN {{ source('retail_platform', 'silver_products') }} p USING (product_id)
LEFT JOIN {{ source('retail_platform', 'silver_stores') }}   s USING (store_id)
WHERE t.status != 'returned'
GROUP BY
    t.transaction_date, t.store_id, s.region,
    t.product_id, p.category, p.subcategory
