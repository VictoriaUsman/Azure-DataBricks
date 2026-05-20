{{ config(materialized = 'view') }}

SELECT
    dp.product_name,
    dp.category,
    dp.subcategory,
    ROUND(SUM(f.net_revenue), 2)               AS revenue,
    SUM(f.quantity)                             AS units_sold,
    ROUND(AVG(f.discount) * 100, 1)            AS avg_discount_pct,
    dp.return_rate,
    RANK() OVER (ORDER BY SUM(f.net_revenue) DESC) AS overall_rank
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} dp USING (product_key)
GROUP BY dp.product_name, dp.category, dp.subcategory, dp.return_rate
ORDER BY revenue DESC
LIMIT 10
