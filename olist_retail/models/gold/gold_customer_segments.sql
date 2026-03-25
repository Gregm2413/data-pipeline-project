-- models/gold/gold_customer_segments.sql
-- RFM-style customer segmentation built on top of silver.customer_journeys
-- Segments customers into actionable business tiers based on behavioral signals

WITH base AS (
    SELECT
        customer_id,
        customer_city,
        customer_state,
        total_sessions,
        total_product_views,
        total_add_to_cart_events,
        total_purchases,
        total_behavioral_revenue,
        session_conversion_rate_pct,
        avg_session_duration_sec,
        customer_lifetime_days,
        ever_converted,
        sessions_before_first_conversion,
        deepest_funnel_stage_name,
        first_seen_at,
        last_seen_at,
        actual_order_count,
        delivered_orders
    FROM {{ source('silver', 'customer_journeys') }}
),

segmented AS (
    SELECT
        *,
        CASE
            WHEN ever_converted = true
                AND total_purchases >= 3
                AND session_conversion_rate_pct >= 50  THEN 'Champion'
            WHEN ever_converted = true
                AND total_purchases >= 2               THEN 'Loyal'
            WHEN ever_converted = true
                AND total_purchases = 1
                AND customer_lifetime_days <= 90       THEN 'New Customer'
            WHEN ever_converted = true
                AND total_purchases = 1
                AND customer_lifetime_days > 90        THEN 'At Risk'
            WHEN ever_converted = false
                AND total_add_to_cart_events >= 1      THEN 'High Intent'
            WHEN ever_converted = false
                AND total_product_views >= 3           THEN 'Browser'
            ELSE 'Cold'
        END AS customer_segment
    FROM base
)

SELECT
    customer_id,
    customer_city,
    customer_state,
    customer_segment,
    total_sessions,
    total_product_views,
    total_add_to_cart_events,
    total_purchases,
    ROUND(COALESCE(total_behavioral_revenue, 0), 2) AS total_revenue,
    ROUND(session_conversion_rate_pct, 2)   AS conversion_rate_pct,
    ROUND(avg_session_duration_sec / 60, 2) AS avg_session_duration_min,
    customer_lifetime_days,
    ever_converted,
    sessions_before_first_conversion,
    deepest_funnel_stage_name,
    actual_order_count,
    delivered_orders,
    first_seen_at,
    last_seen_at,
    CURRENT_TIMESTAMP()                     AS dbt_updated_at
FROM segmented