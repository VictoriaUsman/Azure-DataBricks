{{
    config(materialized = 'table')
}}

WITH product_agg AS (
    SELECT
        t.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_price,
        ROUND(SUM(CASE WHEN t.status = 'completed' THEN t.revenue_net ELSE 0 END), 2) AS revenue_completed,
        ROUND(SUM(CASE WHEN t.status = 'returned'  THEN t.revenue_net ELSE 0 END), 2) AS revenue_returned,
        SUM(t.quantity)                                                                 AS units_sold,
        COUNT(t.transaction_id)                                                         AS transaction_count,
        ROUND(AVG(t.discount), 4)                                                       AS avg_discount,
        COUNT(DISTINCT t.customer_id)                                                   AS unique_buyers
    FROM {{ source('retail_platform', 'silver_transactions') }} t
    LEFT JOIN {{ source('retail_platform', 'silver_products') }} p USING (product_id)
    GROUP BY t.product_id, p.product_name, p.category, p.subcategory, p.unit_price
)

SELECT
    *,
    ROUND(revenue_returned / (revenue_completed + 0.01) * 100, 2) AS return_rate,
    RANK() OVER (
        PARTITION BY category
        ORDER BY revenue_completed DESC
    )                                                               AS category_rank
FROM product_agg
