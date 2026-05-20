{{ config(materialized = 'view') }}

SELECT
    year,
    ROUND(SUM(net_revenue), 2)                                      AS total_revenue,
    COUNT(DISTINCT transaction_id)                                   AS total_transactions,
    COUNT(DISTINCT customer_key)                                     AS unique_customers,
    ROUND(SUM(net_revenue) / COUNT(DISTINCT transaction_id), 2)     AS avg_order_value,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2)                                       AS return_rate_pct
FROM {{ ref('fact_sales') }}
GROUP BY year
ORDER BY year
