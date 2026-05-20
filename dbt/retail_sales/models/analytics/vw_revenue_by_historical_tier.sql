{{ config(materialized = 'view') }}

-- Revenue attributed to the customer tier that was active AT THE TIME of each
-- sale — not the customer's current tier.  fact_sales already resolves the
-- historically-correct customer_key via the SCD2 date-range join, so this view
-- simply groups by the tier from the matched dim_customer version.

SELECT
    dd.year,
    dd.year_month,
    dc.customer_tier,
    dc.segment,
    COUNT(DISTINCT f.transaction_id)   AS transactions,
    ROUND(SUM(f.net_revenue), 2)       AS revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_customer') }} dc
    ON f.customer_key = dc.dbt_scd_id
JOIN {{ ref('dim_date') }}     dd
    ON f.date_key = dd.date_key
GROUP BY dd.year, dd.year_month, dc.customer_tier, dc.segment
ORDER BY dd.year_month, dc.customer_tier
