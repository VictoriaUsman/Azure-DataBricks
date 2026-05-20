{{ config(materialized = 'view') }}

WITH monthly AS (
    SELECT
        year_month,
        year,
        month,
        ROUND(SUM(net_revenue), 2)         AS revenue,
        COUNT(DISTINCT transaction_id)      AS transactions,
        COUNT(DISTINCT customer_key)        AS active_customers
    FROM {{ ref('fact_sales') }}
    GROUP BY year_month, year, month
)

SELECT
    m.*,
    LAG(revenue) OVER (ORDER BY year_month)                    AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year_month))
        / NULLIF(LAG(revenue) OVER (ORDER BY year_month), 0) * 100,
        2
    )                                                           AS mom_growth_pct
FROM monthly m
ORDER BY year_month
