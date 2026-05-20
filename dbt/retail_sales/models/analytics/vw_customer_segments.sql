{{ config(materialized = 'view') }}

-- Joins to the CURRENT dim_customer version only (dbt_valid_to IS NULL).
-- For revenue attributed to historical tiers, use vw_revenue_by_historical_tier.

SELECT
    dc.customer_tier,
    dc.segment,
    COUNT(DISTINCT f.customer_key)           AS customer_count,
    ROUND(SUM(f.net_revenue), 2)             AS total_revenue,
    ROUND(AVG(f.net_revenue), 2)             AS avg_order_value,
    ROUND(
        SUM(f.net_revenue) / SUM(SUM(f.net_revenue)) OVER () * 100,
        2
    )                                         AS revenue_share_pct
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_customer') }} dc
    ON  f.customer_key = dc.dbt_scd_id
    AND dc.dbt_valid_to IS NULL
GROUP BY dc.customer_tier, dc.segment
ORDER BY total_revenue DESC
