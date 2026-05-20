{{ config(materialized = 'view') }}

-- Full SCD2 audit trail: every version of every customer record.
-- total_versions > 1 proves SCD2 history is accumulating.

SELECT
    customer_id,
    customer_name,
    segment,
    city,
    customer_tier,
    CAST(dbt_valid_from AS DATE)  AS effective_start_date,
    CAST(dbt_valid_to   AS DATE)  AS effective_end_date,
    dbt_valid_to IS NULL          AS is_current,
    DATEDIFF(
        COALESCE(CAST(dbt_valid_to AS DATE), CURRENT_DATE()),
        CAST(dbt_valid_from AS DATE)
    )                             AS days_in_version,
    COUNT(*) OVER (PARTITION BY customer_id) AS total_versions
FROM {{ ref('dim_customer') }}
ORDER BY customer_id, dbt_valid_from
