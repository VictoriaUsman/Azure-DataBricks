{{
    config(
        materialized     = 'table',
        partition_by     = [{'field': 'year', 'data_type': 'int'},
                            {'field': 'month', 'data_type': 'int'}],
        post_hook        = "OPTIMIZE {{ this }} ZORDER BY (customer_key, product_key, date_key)",
        file_format      = 'delta',
        tblproperties    = {'delta.enableChangeDataFeed': 'true'}
    )
}}

-- SCD2-aware join: resolve the dim_customer version that was active
-- on each transaction_date so historical facts carry the correct
-- segment / city / customer_tier values at the time of sale.

WITH transactions AS (
    SELECT * FROM {{ source('retail_platform', 'silver_transactions') }}
),

customer_keys AS (
    SELECT
        t.transaction_id,
        dc.dbt_scd_id AS customer_key
    FROM transactions t
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON  t.customer_id = dc.customer_id
        AND CAST(t.transaction_date AS TIMESTAMP) >= dc.dbt_valid_from
        AND CAST(t.transaction_date AS TIMESTAMP) <  COALESCE(dc.dbt_valid_to,
                                                       CAST('9999-12-31' AS TIMESTAMP))
)

SELECT
    t.transaction_id,
    CAST(DATE_FORMAT(t.transaction_date, 'yyyyMMdd') AS INT) AS date_key,
    ck.customer_key,
    dp.product_key,
    ds.store_key,
    t.transaction_date,
    t.quantity,
    t.unit_price,
    t.discount,
    t.total_amount,
    CASE
        WHEN t.status = 'returned' THEN -t.revenue_net
        ELSE t.revenue_net
    END                                                       AS net_revenue,
    t.status,
    t.is_returned,
    t.year,
    t.month,
    t.year_month
FROM transactions t
LEFT JOIN customer_keys ck USING (transaction_id)
LEFT JOIN {{ ref('dim_product') }} dp USING (product_id)
LEFT JOIN {{ ref('dim_store') }}   ds USING (store_id)
