{% snapshot dim_customer %}

{{
    config(
        target_schema  = 'retail_platform',
        unique_key     = 'customer_id',
        strategy       = 'check',
        check_cols     = ['segment', 'city', 'customer_tier'],
        updated_at     = 'snapshot_at',
        invalidate_hard_deletes = false
    )
}}

-- Snapshot source: Silver customer joined with Gold RFM tier.
-- check_cols drives SCD2 history — any change in segment, city, or
-- customer_tier closes the old row and inserts a new active one.
-- customer_name / email / lifetime_value are Type-1: updated in-place.

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.city,
    c.segment,
    COALESCE(g.customer_tier,      'Unknown') AS customer_tier,
    COALESCE(g.lifetime_value,     0.0)       AS lifetime_value,
    COALESCE(g.purchase_frequency, 0)         AS purchase_frequency,
    CURRENT_TIMESTAMP()                        AS snapshot_at
FROM {{ source('retail_platform', 'silver_customers') }} c
LEFT JOIN {{ ref('gold_customer_summary') }} g USING (customer_id)

{% endsnapshot %}
