{{
    config(materialized = 'table')
}}

WITH completed_txn AS (
    SELECT *
    FROM {{ source('retail_platform', 'silver_transactions') }}
    WHERE status = 'completed'
),

latest_date AS (
    SELECT MAX(transaction_date) AS max_date FROM completed_txn
),

rfm_base AS (
    SELECT
        t.customer_id,
        MAX(t.transaction_date)              AS last_purchase_date,
        COUNT(t.transaction_id)              AS purchase_frequency,
        ROUND(SUM(t.revenue_net), 2)         AS lifetime_value,
        ROUND(AVG(t.revenue_net), 2)         AS avg_order_value,
        COUNT(DISTINCT t.product_id)         AS distinct_products,
        COUNT(DISTINCT t.store_id)           AS distinct_stores,
        SUM(t.is_returned)                   AS total_returns,
        DATEDIFF(l.max_date, MAX(t.transaction_date)) AS recency_days
    FROM completed_txn t
    CROSS JOIN latest_date l
    GROUP BY t.customer_id, l.max_date
),

rfm_scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days ASC)       AS r_score,
        NTILE(5) OVER (ORDER BY purchase_frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY lifetime_value ASC)     AS m_score
    FROM rfm_base
)

SELECT
    r.customer_id,
    r.last_purchase_date,
    r.purchase_frequency,
    r.lifetime_value,
    r.avg_order_value,
    r.distinct_products,
    r.distinct_stores,
    r.total_returns,
    r.recency_days,
    r.r_score,
    r.f_score,
    r.m_score,
    r.r_score + r.f_score + r.m_score AS rfm_score,
    CASE
        WHEN r.r_score + r.f_score + r.m_score >= 13 THEN 'Champions'
        WHEN r.r_score + r.f_score + r.m_score >= 10 THEN 'Loyal'
        WHEN r.r_score + r.f_score + r.m_score >= 7  THEN 'Potential'
        ELSE 'At Risk'
    END                                AS customer_tier,
    c.segment,
    c.city
FROM rfm_scored r
LEFT JOIN {{ source('retail_platform', 'silver_customers') }} c USING (customer_id)
