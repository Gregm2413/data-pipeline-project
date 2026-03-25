-- models/gold/gold_executive_summary.sql
-- Daily executive KPI summary combining funnel and customer metrics
-- Designed for dashboard consumption

WITH funnel AS (
    SELECT
        session_date,
        total_sessions,
        product_views,
        add_to_carts,
        purchases,
        daily_conversion_rate_pct
    FROM {{ source('silver', 'funnel_daily') }}
),

customer_stats AS (
    SELECT
        COUNT(DISTINCT customer_id)                                         AS total_customers,
        SUM(CASE WHEN ever_converted = true THEN 1 ELSE 0 END)             AS converted_customers,
        SUM(CASE WHEN customer_segment = 'Champion' THEN 1 ELSE 0 END)     AS champion_customers,
        SUM(CASE WHEN customer_segment = 'High Intent' THEN 1 ELSE 0 END)  AS high_intent_customers,
        ROUND(SUM(total_revenue), 2)                                        AS total_revenue,
        ROUND(AVG(avg_session_duration_min), 2)                             AS avg_session_duration_min
    FROM {{ ref('gold_customer_segments') }}
),

daily_summary AS (
    SELECT
        f.session_date,
        f.total_sessions,
        f.product_views,
        f.add_to_carts,
        f.purchases,
        f.daily_conversion_rate_pct,
        ROUND(f.product_views * 100.0 / NULLIF(f.total_sessions, 0), 2)    AS product_view_rate_pct,
        ROUND(f.add_to_carts * 100.0 / NULLIF(f.product_views, 0), 2)      AS cart_rate_pct,
        ROUND(f.purchases * 100.0 / NULLIF(f.add_to_carts, 0), 2)          AS checkout_rate_pct
    FROM funnel f
)

SELECT
    d.session_date,
    d.total_sessions,
    d.product_views,
    d.add_to_carts,
    d.purchases,
    d.daily_conversion_rate_pct,
    d.product_view_rate_pct,
    d.cart_rate_pct,
    d.checkout_rate_pct,
    c.total_customers,
    c.converted_customers,
    c.champion_customers,
    c.high_intent_customers,
    c.total_revenue,
    c.avg_session_duration_min,
    CURRENT_TIMESTAMP()                                                     AS dbt_updated_at
FROM daily_summary d
CROSS JOIN customer_stats c
ORDER BY d.session_date