{{
    config(materialized = 'table')
}}

SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)          AS date_key,
    d                                                 AS full_date,
    YEAR(d)                                           AS year,
    QUARTER(d)                                        AS quarter,
    MONTH(d)                                          AS month,
    DATE_FORMAT(d, 'MMMM')                           AS month_name,
    DATE_FORMAT(d, 'MMM')                            AS month_abbr,
    WEEKOFYEAR(d)                                     AS week_of_year,
    DAYOFMONTH(d)                                     AS day_of_month,
    DAYOFWEEK(d)                                      AS day_of_week,
    DATE_FORMAT(d, 'EEEE')                           AS day_name,
    CASE WHEN DAYOFWEEK(d) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
    DATE_FORMAT(d, 'yyyy-MM')                        AS year_month,
    CONCAT('Q', QUARTER(d), '-', YEAR(d))            AS quarter_label
FROM (
    SELECT EXPLODE(
        SEQUENCE(DATE '2023-01-01', DATE '2024-12-31', INTERVAL 1 DAY)
    ) AS d
)
