{{ config(materialized = 'view') }}

WITH first_purchase AS (
    SELECT customer_key, MIN(transaction_date) AS first_date
    FROM {{ ref('fact_sales') }}
    GROUP BY customer_key
)

SELECT
    f.year_month,
    COUNT(DISTINCT CASE WHEN f.transaction_date = fp.first_date
                        THEN f.customer_key END) AS new_customers,
    COUNT(DISTINCT CASE WHEN f.transaction_date > fp.first_date
                        THEN f.customer_key END) AS returning_customers
FROM {{ ref('fact_sales') }} f
JOIN first_purchase fp USING (customer_key)
GROUP BY f.year_month
ORDER BY f.year_month
