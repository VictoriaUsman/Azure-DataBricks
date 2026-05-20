{{ config(materialized = 'view') }}

SELECT
    ds.region,
    ds.store_name,
    ds.city,
    ROUND(SUM(f.net_revenue), 2)              AS revenue,
    COUNT(DISTINCT f.transaction_id)           AS transactions,
    COUNT(DISTINCT f.customer_key)             AS unique_customers,
    RANK() OVER (
        PARTITION BY ds.region
        ORDER BY SUM(f.net_revenue) DESC
    )                                          AS region_rank
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_store') }} ds USING (store_key)
GROUP BY ds.region, ds.store_name, ds.city
ORDER BY ds.region, region_rank
