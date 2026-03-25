-- models/gold/gold_product_engagement.sql
-- Product-level engagement metrics: views, cart adds, and purchase rates
-- Built on top of silver.sessions

WITH product_events AS (
    SELECT
        EXPLODE(products_interacted) AS product_id,
        session_id,
        customer_id,
        converted,
        session_revenue,
        max_funnel_stage_reached
    FROM {{ source('silver', 'sessions') }}
    WHERE products_interacted IS NOT NULL
),

product_metrics AS (
    SELECT
        product_id,
        COUNT(DISTINCT session_id)                                          AS total_sessions,
        COUNT(DISTINCT customer_id)                                         AS unique_customers,
        SUM(CASE WHEN max_funnel_stage_reached >= 2 THEN 1 ELSE 0 END)     AS product_view_sessions,
        SUM(CASE WHEN max_funnel_stage_reached >= 3 THEN 1 ELSE 0 END)     AS add_to_cart_sessions,
        SUM(CASE WHEN converted = true THEN 1 ELSE 0 END)                  AS converting_sessions,
        ROUND(SUM(session_revenue), 2)                                      AS total_revenue,
        ROUND(AVG(session_revenue), 2)                                      AS avg_session_revenue
    FROM product_events
    GROUP BY product_id
)

SELECT
    product_id,
    total_sessions,
    unique_customers,
    product_view_sessions,
    add_to_cart_sessions,
    converting_sessions,
    total_revenue,
    avg_session_revenue,
    ROUND(add_to_cart_sessions * 100.0 / NULLIF(product_view_sessions, 0), 2)   AS cart_rate_pct,
    ROUND(converting_sessions * 100.0 / NULLIF(product_view_sessions, 0), 2)    AS purchase_rate_pct,
    CURRENT_TIMESTAMP()                                                          AS dbt_updated_at
FROM product_metrics
ORDER BY total_revenue DESC