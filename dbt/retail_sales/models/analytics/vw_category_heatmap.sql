{{ config(materialized = 'view') }}

SELECT
    dp.category,
    dd.month_abbr,
    dd.year,
    ROUND(SUM(f.net_revenue), 2) AS revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} dp USING (product_key)
JOIN {{ ref('dim_date') }}    dd USING (date_key)
GROUP BY dp.category, dd.month_abbr, dd.year
ORDER BY dd.year, dp.category, dd.month_abbr
