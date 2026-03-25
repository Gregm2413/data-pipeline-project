-- models/gold/gold_funnel_performance.sql
-- Weekly and monthly funnel rollups with conversion rate trends
-- Built on top of silver.funnel_daily

WITH daily AS (
    SELECT
        session_date,
        total_sessions,
        product_views,
        add_to_carts,
        purchases,
        daily_conversion_rate_pct
    FROM {{ source('silver', 'funnel_daily') }}
),

weekly AS (
    SELECT
        DATE_TRUNC('week', session_date)        AS period_start,
        'week'                                  AS period_type,
        COUNT(DISTINCT session_date)            AS days_in_period,
        SUM(total_sessions)                     AS total_sessions,
        SUM(product_views)                      AS product_views,
        SUM(add_to_carts)                       AS add_to_carts,
        SUM(purchases)                          AS purchases,
        ROUND(AVG(daily_conversion_rate_pct), 2) AS avg_daily_conversion_rate_pct,
        ROUND(SUM(purchases) * 100.0 / NULLIF(SUM(total_sessions), 0), 2) AS period_conversion_rate_pct
    FROM daily
    GROUP BY DATE_TRUNC('week', session_date)
),

monthly AS (
    SELECT
        DATE_TRUNC('month', session_date)       AS period_start,
        'month'                                 AS period_type,
        COUNT(DISTINCT session_date)            AS days_in_period,
        SUM(total_sessions)                     AS total_sessions,
        SUM(product_views)                      AS product_views,
        SUM(add_to_carts)                       AS add_to_carts,
        SUM(purchases)                          AS purchases,
        ROUND(AVG(daily_conversion_rate_pct), 2) AS avg_daily_conversion_rate_pct,
        ROUND(SUM(purchases) * 100.0 / NULLIF(SUM(total_sessions), 0), 2) AS period_conversion_rate_pct
    FROM daily
    GROUP BY DATE_TRUNC('month', session_date)
)

SELECT * FROM weekly
UNION ALL
SELECT * FROM monthly
ORDER BY period_type, period_start