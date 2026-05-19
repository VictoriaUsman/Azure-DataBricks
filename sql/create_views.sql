-- =============================================================================
-- Standalone SQL Views — Azure Synapse Analytics / Azure SQL equivalent
-- These views are identical to the Databricks SQL views in notebook 05.
-- In a full Azure deployment, run this script against Synapse SQL Pool after
-- loading Gold Delta tables via PolyBase or COPY INTO.
-- =============================================================================

USE retail_platform;

-- Executive summary
CREATE OR REPLACE VIEW vw_exec_summary AS
SELECT
    year,
    ROUND(SUM(net_revenue), 2)                                       AS total_revenue,
    COUNT(DISTINCT transaction_id)                                   AS total_transactions,
    COUNT(DISTINCT customer_key)                                     AS unique_customers,
    ROUND(SUM(net_revenue) / COUNT(DISTINCT transaction_id), 2)     AS avg_order_value,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2)                                       AS return_rate_pct
FROM  fact_sales
GROUP BY year;

-- Monthly revenue trend
CREATE OR REPLACE VIEW vw_revenue_trend AS
WITH monthly AS (
    SELECT
        year_month,
        year,
        month,
        ROUND(SUM(net_revenue), 2)        AS revenue,
        COUNT(DISTINCT transaction_id)    AS transactions
    FROM  fact_sales
    GROUP BY year_month, year, month
)
SELECT
    m.*,
    LAG(revenue) OVER (ORDER BY year_month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year_month))
        / LAG(revenue) OVER (ORDER BY year_month) * 100, 2
    )                                        AS mom_growth_pct
FROM monthly m;

-- Customer segmentation
CREATE OR REPLACE VIEW vw_customer_segments AS
SELECT
    dc.customer_tier,
    dc.segment,
    COUNT(DISTINCT f.customer_key)          AS customer_count,
    ROUND(SUM(f.net_revenue), 2)           AS total_revenue,
    ROUND(AVG(f.net_revenue), 2)           AS avg_order_value
FROM  fact_sales    f
JOIN  dim_customer  dc ON f.customer_key = dc.customer_key
GROUP BY dc.customer_tier, dc.segment;

-- Product performance
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    dp.product_name,
    dp.category,
    ROUND(SUM(f.net_revenue), 2)           AS revenue,
    SUM(f.quantity)                        AS units_sold,
    dp.return_rate,
    RANK() OVER (ORDER BY SUM(f.net_revenue) DESC) AS overall_rank
FROM  fact_sales   f
JOIN  dim_product  dp ON f.product_key = dp.product_key
GROUP BY dp.product_name, dp.category, dp.return_rate;

-- Regional store ranking
CREATE OR REPLACE VIEW vw_store_ranking AS
SELECT
    ds.region,
    ds.store_name,
    ds.city,
    ROUND(SUM(f.net_revenue), 2)          AS revenue,
    RANK() OVER (
        PARTITION BY ds.region
        ORDER BY SUM(f.net_revenue) DESC
    )                                     AS region_rank
FROM  fact_sales  f
JOIN  dim_store   ds ON f.store_key = ds.store_key
GROUP BY ds.region, ds.store_name, ds.city;

-- New vs returning customers
CREATE OR REPLACE VIEW vw_customer_acquisition AS
WITH first_purchase AS (
    SELECT customer_key, MIN(transaction_date) AS first_date
    FROM   fact_sales
    GROUP BY customer_key
)
SELECT
    f.year_month,
    COUNT(DISTINCT CASE WHEN f.transaction_date = fp.first_date
                        THEN f.customer_key END) AS new_customers,
    COUNT(DISTINCT CASE WHEN f.transaction_date > fp.first_date
                        THEN f.customer_key END) AS returning_customers
FROM  fact_sales     f
JOIN  first_purchase fp ON f.customer_key = fp.customer_key
GROUP BY f.year_month;
