{{
    config(
        materialized = 'table',
        partition_by = {'field': 'year', 'data_type': 'int'}
    )
}}

WITH store_agg AS (
    SELECT
        t.store_id,
        s.store_name,
        s.city,
        s.region,
        t.year_month,
        t.year,
        t.month,
        ROUND(SUM(t.revenue_net), 2)          AS revenue,
        SUM(t.quantity)                        AS units_sold,
        COUNT(t.transaction_id)                AS transaction_count,
        COUNT(DISTINCT t.customer_id)          AS unique_customers,
        ROUND(AVG(t.revenue_net), 2)           AS avg_transaction_value
    FROM {{ source('retail_platform', 'silver_transactions') }} t
    LEFT JOIN {{ source('retail_platform', 'silver_stores') }} s USING (store_id)
    WHERE t.status != 'returned'
    GROUP BY t.store_id, s.store_name, s.city, s.region, t.year_month, t.year, t.month
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY region, year_month
        ORDER BY revenue DESC
    ) AS region_rank
FROM store_agg
