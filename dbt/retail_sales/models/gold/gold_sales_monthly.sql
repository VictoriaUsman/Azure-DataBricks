{{
    config(
        materialized = 'table',
        partition_by = {'field': 'year', 'data_type': 'int'}
    )
}}

WITH monthly_base AS (
    SELECT
        t.year_month,
        t.year,
        t.month,
        p.category,
        ROUND(SUM(t.revenue_net), 2)         AS revenue,
        SUM(t.quantity)                       AS units_sold,
        COUNT(t.transaction_id)               AS transaction_count,
        COUNT(DISTINCT t.customer_id)         AS unique_customers
    FROM {{ source('retail_platform', 'silver_transactions') }} t
    LEFT JOIN {{ source('retail_platform', 'silver_products') }} p USING (product_id)
    WHERE t.status != 'returned'
    GROUP BY t.year_month, t.year, t.month, p.category
)

SELECT
    year_month,
    year,
    month,
    category,
    revenue,
    units_sold,
    transaction_count,
    unique_customers,
    LAG(revenue) OVER (
        PARTITION BY category ORDER BY year_month
    )                                                               AS revenue_prev_month,
    ROUND(
        (revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY year_month))
        / NULLIF(LAG(revenue) OVER (PARTITION BY category ORDER BY year_month), 0) * 100,
        2
    )                                                               AS revenue_mom_pct
FROM monthly_base
