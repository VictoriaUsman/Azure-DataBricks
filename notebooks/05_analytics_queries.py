# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics Queries — KPI Layer
# MAGIC
# MAGIC **Purpose:** Business-facing SQL queries that Power BI (or any BI tool) connects to.
# MAGIC In a real deployment these would be Databricks SQL views or Azure Synapse views.
# MAGIC
# MAGIC **KPIs covered:**
# MAGIC 1. Executive Dashboard (revenue, growth, top categories)
# MAGIC 2. Sales Trend Analysis (MoM, YoY)
# MAGIC 3. Customer Segmentation (RFM tiers)
# MAGIC 4. Product Performance (top sellers, return rates)
# MAGIC 5. Regional Store Rankings
# MAGIC 6. Cohort Retention Proxy
# MAGIC 7. Customer Version History (SCD2 audit trail)
# MAGIC
# MAGIC **SCD2 note:** All views that join `dim_customer` filter on `is_current = true`
# MAGIC to use the customer's current attributes. Use `vw_customer_history` to query
# MAGIC attributes as they were at the time of a specific transaction.

# COMMAND ----------

spark.sql("USE retail_platform")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1 — Executive Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_exec_summary AS
# MAGIC SELECT
# MAGIC     year,
# MAGIC     ROUND(SUM(net_revenue), 2)                                          AS total_revenue,
# MAGIC     COUNT(DISTINCT transaction_id)                                      AS total_transactions,
# MAGIC     COUNT(DISTINCT customer_key)                                        AS unique_customers,
# MAGIC     ROUND(SUM(net_revenue) / COUNT(DISTINCT transaction_id), 2)        AS avg_order_value,
# MAGIC     ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END)
# MAGIC           / COUNT(*) * 100, 2)                                          AS return_rate_pct
# MAGIC FROM  fact_sales
# MAGIC GROUP BY year
# MAGIC ORDER BY year;
# MAGIC
# MAGIC SELECT * FROM vw_exec_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 2 — Monthly Revenue Trend with Growth Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_revenue_trend AS
# MAGIC WITH monthly AS (
# MAGIC     SELECT
# MAGIC         year_month,
# MAGIC         year,
# MAGIC         month,
# MAGIC         ROUND(SUM(net_revenue), 2)          AS revenue,
# MAGIC         COUNT(DISTINCT transaction_id)       AS transactions,
# MAGIC         COUNT(DISTINCT customer_key)         AS active_customers
# MAGIC     FROM  fact_sales
# MAGIC     GROUP BY year_month, year, month
# MAGIC )
# MAGIC SELECT
# MAGIC     m.*,
# MAGIC     LAG(revenue) OVER (ORDER BY year_month)  AS prev_month_revenue,
# MAGIC     ROUND(
# MAGIC         (revenue - LAG(revenue) OVER (ORDER BY year_month))
# MAGIC         / LAG(revenue) OVER (ORDER BY year_month) * 100, 2
# MAGIC     )                                         AS mom_growth_pct
# MAGIC FROM monthly m
# MAGIC ORDER BY year_month;
# MAGIC
# MAGIC SELECT * FROM vw_revenue_trend

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3 — Customer Tier Breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_customer_segments AS
# MAGIC SELECT
# MAGIC     dc.customer_tier,
# MAGIC     dc.segment,
# MAGIC     COUNT(DISTINCT f.customer_key)           AS customer_count,
# MAGIC     ROUND(SUM(f.net_revenue), 2)             AS total_revenue,
# MAGIC     ROUND(AVG(f.net_revenue), 2)             AS avg_order_value,
# MAGIC     ROUND(SUM(f.net_revenue)
# MAGIC           / SUM(SUM(f.net_revenue)) OVER () * 100, 2) AS revenue_share_pct
# MAGIC FROM  fact_sales      f
# MAGIC JOIN  dim_customer    dc ON f.customer_key = dc.customer_key
# MAGIC                        AND dc.is_current = true
# MAGIC GROUP BY dc.customer_tier, dc.segment
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT * FROM vw_customer_segments

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 4 — Top 10 Products by Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_top_products AS
# MAGIC SELECT
# MAGIC     dp.product_name,
# MAGIC     dp.category,
# MAGIC     dp.subcategory,
# MAGIC     ROUND(SUM(f.net_revenue), 2)              AS revenue,
# MAGIC     SUM(f.quantity)                           AS units_sold,
# MAGIC     ROUND(AVG(f.discount) * 100, 1)          AS avg_discount_pct,
# MAGIC     dp.return_rate,
# MAGIC     RANK() OVER (ORDER BY SUM(f.net_revenue) DESC) AS overall_rank
# MAGIC FROM  fact_sales    f
# MAGIC JOIN  dim_product   dp ON f.product_key = dp.product_key
# MAGIC GROUP BY dp.product_name, dp.category, dp.subcategory, dp.return_rate
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT * FROM vw_top_products

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 5 — Regional Store Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_store_ranking AS
# MAGIC SELECT
# MAGIC     ds.region,
# MAGIC     ds.store_name,
# MAGIC     ds.city,
# MAGIC     ROUND(SUM(f.net_revenue), 2)              AS revenue,
# MAGIC     COUNT(DISTINCT f.transaction_id)          AS transactions,
# MAGIC     COUNT(DISTINCT f.customer_key)            AS unique_customers,
# MAGIC     RANK() OVER (PARTITION BY ds.region
# MAGIC                  ORDER BY SUM(f.net_revenue) DESC) AS region_rank
# MAGIC FROM  fact_sales   f
# MAGIC JOIN  dim_store    ds ON f.store_key = ds.store_key
# MAGIC GROUP BY ds.region, ds.store_name, ds.city
# MAGIC ORDER BY ds.region, region_rank;
# MAGIC
# MAGIC SELECT * FROM vw_store_ranking

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 6 — Category Heatmap (Month × Category)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_category_heatmap AS
# MAGIC SELECT
# MAGIC     dp.category,
# MAGIC     dd.month_abbr,
# MAGIC     dd.year,
# MAGIC     ROUND(SUM(f.net_revenue), 2) AS revenue
# MAGIC FROM  fact_sales  f
# MAGIC JOIN  dim_product dp ON f.product_key = dp.product_key
# MAGIC JOIN  dim_date    dd ON f.date_key     = dd.date_key
# MAGIC GROUP BY dp.category, dd.month_abbr, dd.year
# MAGIC ORDER BY dd.year, dp.category, dd.month_abbr;
# MAGIC
# MAGIC SELECT * FROM vw_category_heatmap WHERE year = 2024 ORDER BY category, month_abbr

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 7 — New vs. Returning Customers per Month

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vw_customer_acquisition AS
# MAGIC WITH first_purchase AS (
# MAGIC     SELECT customer_key, MIN(transaction_date) AS first_date
# MAGIC     FROM   fact_sales
# MAGIC     GROUP BY customer_key
# MAGIC )
# MAGIC SELECT
# MAGIC     f.year_month,
# MAGIC     COUNT(DISTINCT CASE WHEN f.transaction_date = fp.first_date
# MAGIC                         THEN f.customer_key END) AS new_customers,
# MAGIC     COUNT(DISTINCT CASE WHEN f.transaction_date > fp.first_date
# MAGIC                         THEN f.customer_key END) AS returning_customers
# MAGIC FROM  fact_sales     f
# MAGIC JOIN  first_purchase fp ON f.customer_key = fp.customer_key
# MAGIC GROUP BY f.year_month
# MAGIC ORDER BY f.year_month;
# MAGIC
# MAGIC SELECT * FROM vw_customer_acquisition

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 8 — Customer Version History (SCD2 Audit)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows every historical version of a customer's tracked attributes.
# MAGIC -- Useful for auditing attribute changes and for historised reporting.
# MAGIC CREATE OR REPLACE VIEW vw_customer_history AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     city,
# MAGIC     customer_tier,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current,
# MAGIC     DATEDIFF(
# MAGIC         COALESCE(effective_end_date, CURRENT_DATE()),
# MAGIC         effective_start_date
# MAGIC     )                          AS days_in_version,
# MAGIC     COUNT(*) OVER (PARTITION BY customer_id) AS total_versions
# MAGIC FROM  dim_customer
# MAGIC ORDER BY customer_id, effective_start_date;
# MAGIC
# MAGIC -- Sample: customers who changed tier at least once
# MAGIC SELECT * FROM vw_customer_history WHERE total_versions > 1 LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Historised revenue: revenue attributed to the customer attributes
# MAGIC -- that were TRUE at the time of each sale (not current attributes).
# MAGIC -- This is the correct way to analyse "revenue by tier" over time.
# MAGIC CREATE OR REPLACE VIEW vw_revenue_by_historical_tier AS
# MAGIC SELECT
# MAGIC     dd.year,
# MAGIC     dd.year_month,
# MAGIC     dc.customer_tier,
# MAGIC     dc.segment,
# MAGIC     COUNT(DISTINCT f.transaction_id)   AS transactions,
# MAGIC     ROUND(SUM(f.net_revenue), 2)       AS revenue
# MAGIC FROM  fact_sales   f
# MAGIC JOIN  dim_customer dc ON f.customer_key = dc.customer_key
# MAGIC JOIN  dim_date     dd ON f.date_key     = dd.date_key
# MAGIC GROUP BY dd.year, dd.year_month, dc.customer_tier, dc.segment
# MAGIC ORDER BY dd.year_month, dc.customer_tier;
# MAGIC
# MAGIC SELECT * FROM vw_revenue_by_historical_tier ORDER BY year_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Power BI Connection Instructions
# MAGIC
# MAGIC To connect Power BI to these views:
# MAGIC 1. In Databricks: **SQL Warehouses** → start a SQL warehouse → copy the **Server hostname** and **HTTP path**
# MAGIC 2. In Power BI Desktop: **Get Data** → **Databricks** → paste hostname + HTTP path
# MAGIC 3. Select database `retail_platform`, import the `vw_*` views
# MAGIC 4. Build reports on top of pre-aggregated views for best performance
# MAGIC
# MAGIC **In a real Azure deployment:** replace Databricks SQL with Azure Synapse Analytics
# MAGIC SQL Pool — the views and queries are identical, only the connection string changes.
