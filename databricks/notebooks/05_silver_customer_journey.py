# Databricks notebook source

# MAGIC %md
# MAGIC # 05 Silver — Customer Journey Reconstruction
# MAGIC
# MAGIC **Purpose:** Stitches cross-session behavioral history per customer into a single
# MAGIC journey profile, replicating what Adobe Customer Journey Analytics provides —
# MAGIC but built from scratch in PySpark on Delta Lake.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Aggregates all sessions per customer into a unified journey timeline
# MAGIC - Computes first-touch / last-touch attribution
# MAGIC - Measures time-to-purchase, sessions-before-conversion, and inter-session cadence
# MAGIC - Produces a customer-level behavioral feature table suitable for Phase 6 ML (churn, LTV)
# MAGIC - Joins behavioral data with Olist transaction data for ground-truth enrichment
# MAGIC
# MAGIC **CJA Parallel:** This is equivalent to Adobe CJA's Person-level container analysis —
# MAGIC stitching Event and Session containers up to the Person level to measure the full
# MAGIC lifecycle journey across visits.
# MAGIC
# MAGIC **Inputs:**
# MAGIC   `olist.silver.sessions`,
# MAGIC   `olist.silver.session_events`,
# MAGIC   `olist.bronze.customers`,
# MAGIC   `olist.bronze.orders`
# MAGIC
# MAGIC **Outputs:**
# MAGIC   `olist.silver.customer_journeys`
# MAGIC   `olist.silver.customer_journey_sessions` (session-level with journey context)

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, sum, count, countDistinct, lit,
    min, max, avg, first, last,
    unix_timestamp, to_date, datediff,
    collect_list, collect_set, sort_array, struct,
    round as spark_round, coalesce, lag,
    row_number, dense_rank, ntile
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver & Bronze Tables

# COMMAND ----------

sessions       = spark.table("olist.silver.sessions")
session_events = spark.table("olist.silver.session_events")
customers      = spark.table("olist.bronze.customers")
orders         = spark.table("olist.bronze.orders")

print(f"Sessions: {sessions.count()}")
print(f"Customers (bronze): {customers.count()}")
print(f"Orders (bronze):    {orders.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer-Level Session Aggregation
# MAGIC
# MAGIC Roll all sessions up to the customer level.
# MAGIC This is the "Person container" in CJA terminology.

# COMMAND ----------

customer_window = Window.partitionBy("customer_id").orderBy("session_start")
customer_window_desc = Window.partitionBy("customer_id").orderBy(col("session_start").desc())

# Tag each session with its rank in the customer's journey
sessions_ranked = sessions.withColumn(
    "session_rank",
    row_number().over(customer_window)
).withColumn(
    "session_rank_desc",
    row_number().over(customer_window_desc)
).withColumn(
    "is_first_session",
    (col("session_rank") == 1)
).withColumn(
    "is_last_session",
    (col("session_rank_desc") == 1)
).withColumn(
    "days_since_prev_session",
    datediff(
        to_date("session_start"),
        to_date(lag("session_start", 1).over(customer_window))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. First-Touch and Last-Touch Attribution
# MAGIC
# MAGIC First-touch: what funnel stage did the customer reach on their FIRST session?
# MAGIC Last-touch: what was the most recent session's funnel stage?
# MAGIC This is a simplified attribution model — Phase 6 ML will build on this.

# COMMAND ----------

first_touch = (
    sessions_ranked
    .filter(col("is_first_session") == True)
    .select(
        "customer_id",
        col("session_id").alias("first_session_id"),
        col("session_start").alias("first_session_start"),
        col("funnel_stage_name").alias("first_touch_stage"),
        col("converted").alias("first_session_converted"),
        col("unique_products_viewed").alias("first_session_products_viewed"),
        col("session_duration_seconds").alias("first_session_duration_sec")
    )
)

last_touch = (
    sessions_ranked
    .filter(col("is_last_session") == True)
    .select(
        "customer_id",
        col("session_id").alias("last_session_id"),
        col("session_start").alias("last_session_start"),
        col("funnel_stage_name").alias("last_touch_stage"),
        col("converted").alias("last_session_converted"),
        col("session_revenue").alias("last_session_revenue")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Customer-Level Aggregation

# COMMAND ----------

customer_journey = sessions_ranked.groupBy("customer_id").agg(

    # Volume metrics
    count("session_id").alias("total_sessions"),
    sum("total_events").alias("total_events"),
    sum("product_views").alias("total_product_views"),
    sum("add_to_cart_events").alias("total_add_to_cart_events"),
    sum("purchases").alias("total_purchases"),
    sum("page_views").alias("total_page_views"),

    # Temporal metrics
    min("session_start").alias("first_seen_at"),
    max("session_end").alias("last_seen_at"),
    sum(coalesce(col("session_duration_seconds"), lit(0))).alias("total_session_seconds"),
    avg(coalesce(col("days_since_prev_session"), lit(0))).alias("avg_days_between_sessions"),

    # Breadth metrics
    sum("unique_products_viewed").alias("total_unique_products_viewed"),
    sum("session_revenue").alias("total_behavioral_revenue"),

    # Conversion metrics
    sum(when(col("converted") == True, 1).otherwise(0)).alias("converting_sessions"),
    max(col("max_funnel_stage_reached")).alias("deepest_funnel_stage_ever"),

    # Max funnel name (derived below)
    max(col("max_funnel_stage_reached")).alias("_max_stage_int")

).withColumn(
    "customer_lifetime_days",
    datediff(to_date("last_seen_at"), to_date("first_seen_at"))
).withColumn(
    "ever_converted",
    (col("total_purchases") > 0)
).withColumn(
    "session_conversion_rate_pct",
    spark_round(
        col("converting_sessions") * 100.0 / col("total_sessions"), 2
    )
).withColumn(
    "avg_session_duration_sec",
    spark_round(
        col("total_session_seconds") / col("total_sessions"), 1
    )
).withColumn(
    "deepest_funnel_stage_name",
    when(col("_max_stage_int") == 4, "purchase")
    .when(col("_max_stage_int") == 3, "add_to_cart")
    .when(col("_max_stage_int") == 2, "product_view")
    .when(col("_max_stage_int") == 1, "page_view")
    .otherwise("unknown")
).drop("_max_stage_int")

print(f"Customer journeys built: {customer_journey.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Sessions-Before-First-Conversion
# MAGIC
# MAGIC For customers who eventually converted, how many sessions did it take?
# MAGIC This is a key CJA metric — customer journey length to first purchase.

# COMMAND ----------

# Find the session number of the first converting session per customer
first_conversion = (
    sessions_ranked
    .filter(col("converted") == True)
    .groupBy("customer_id")
    .agg(min("session_rank").alias("sessions_before_first_conversion"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join with Olist Transaction Data
# MAGIC
# MAGIC Enrich behavioral data with actual order ground-truth from the bronze layer.
# MAGIC This allows us to validate that simulated behavioral events align with
# MAGIC real transaction records — and creates a richer feature set for Phase 6 ML.

# COMMAND ----------

# Olist customer-level order aggregation from bronze
customer_orders = orders.groupBy("customer_id").agg(
    count("order_id").alias("actual_order_count"),
    min("order_purchase_timestamp").alias("first_order_at"),
    max("order_purchase_timestamp").alias("last_order_at"),
    countDistinct(
        when(col("order_status") == "delivered", col("order_id"))
    ).alias("delivered_orders")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Assemble Final Customer Journey Table

# COMMAND ----------

customer_journey_final = (
    customer_journey
    .join(first_touch, on="customer_id", how="left")
    .join(last_touch, on="customer_id", how="left")
    .join(first_conversion, on="customer_id", how="left")
    .join(customer_orders, on="customer_id", how="left")
    # Customers from bronze who may have no behavioral events
    .join(customers.select("customer_id", "customer_city", "customer_state"),
          on="customer_id", how="left")
)

print(f"Final customer journey rows: {customer_journey_final.count()}")
customer_journey_final.show(5, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Session-Level Table with Journey Context
# MAGIC
# MAGIC Add cross-session journey context to each session row.
# MAGIC Useful for cohort analysis and ML feature engineering.

# COMMAND ----------

journey_context = customer_journey_final.select(
    "customer_id",
    "total_sessions",
    "ever_converted",
    "first_seen_at",
    "deepest_funnel_stage_name"
)

session_journey_enriched = (
    sessions_ranked
    .join(journey_context, on="customer_id", how="left")
    .select(
        "session_id",
        "customer_id",
        "session_rank",
        "session_start",
        "session_end",
        "session_duration_seconds",
        "total_events",
        "product_views",
        "add_to_cart_events",
        "purchases",
        "converted",
        "funnel_stage_name",
        "session_revenue",
        "is_first_session",
        "is_last_session",
        "days_since_prev_session",
        # Journey-level context
        col("total_sessions").alias("customer_total_sessions"),
        col("ever_converted").alias("customer_ever_converted"),
        col("deepest_funnel_stage_name").alias("customer_deepest_stage")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Silver Tables

# COMMAND ----------

(
    customer_journey_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.customer_journeys")
)
print("✅ olist.silver.customer_journeys written")

# COMMAND ----------

(
    session_journey_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.customer_journey_sessions")
)
print("✅ olist.silver.customer_journey_sessions written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CJA-Style Summary Analytics
# MAGIC
# MAGIC These queries mirror the Person-level metrics surfaced in Adobe CJA dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CJA: Person-level engagement summary
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT customer_id)                          AS total_customers,
# MAGIC     ROUND(AVG(total_sessions), 2)                       AS avg_sessions_per_customer,
# MAGIC     ROUND(AVG(total_events), 1)                         AS avg_events_per_customer,
# MAGIC     ROUND(AVG(total_unique_products_viewed), 2)         AS avg_products_viewed,
# MAGIC     SUM(CAST(ever_converted AS INT))                    AS customers_who_purchased,
# MAGIC     ROUND(
# MAGIC         SUM(CAST(ever_converted AS INT)) * 100.0 /
# MAGIC         COUNT(DISTINCT customer_id), 2
# MAGIC     )                                                   AS customer_conversion_rate_pct,
# MAGIC     ROUND(AVG(customer_lifetime_days), 1)               AS avg_customer_lifetime_days
# MAGIC FROM olist.silver.customer_journeys;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CJA: Sessions required before first conversion
# MAGIC SELECT
# MAGIC     sessions_before_first_conversion,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_converters
# MAGIC FROM olist.silver.customer_journeys
# MAGIC WHERE sessions_before_first_conversion IS NOT NULL
# MAGIC GROUP BY sessions_before_first_conversion
# MAGIC ORDER BY sessions_before_first_conversion;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CJA: First-touch stage distribution
# MAGIC -- What funnel stage did customers reach on their very first visit?
# MAGIC SELECT
# MAGIC     first_touch_stage,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     SUM(CAST(ever_converted AS INT)) AS converted_customers,
# MAGIC     ROUND(
# MAGIC         SUM(CAST(ever_converted AS INT)) * 100.0 / COUNT(*), 2
# MAGIC     ) AS conversion_rate_from_this_first_touch
# MAGIC FROM olist.silver.customer_journeys
# MAGIC WHERE first_touch_stage IS NOT NULL
# MAGIC GROUP BY first_touch_stage
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CJA: Customer engagement segments (behavioral cohorts)
# MAGIC -- Useful as input to Phase 6 churn/LTV models
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN total_sessions = 1 AND ever_converted = false  THEN 'single_session_bounced'
# MAGIC         WHEN total_sessions = 1 AND ever_converted = true   THEN 'single_session_converted'
# MAGIC         WHEN total_sessions > 1 AND ever_converted = false  THEN 'multi_session_not_converted'
# MAGIC         WHEN total_sessions > 1 AND ever_converted = true   THEN 'multi_session_converted'
# MAGIC         ELSE 'unknown'
# MAGIC     END                             AS engagement_segment,
# MAGIC     COUNT(*)                        AS customer_count,
# MAGIC     ROUND(AVG(total_sessions), 2)   AS avg_sessions,
# MAGIC     ROUND(AVG(total_events), 1)     AS avg_events,
# MAGIC     ROUND(AVG(total_behavioral_revenue), 2) AS avg_revenue
# MAGIC FROM olist.silver.customer_journeys
# MAGIC GROUP BY engagement_segment
# MAGIC ORDER BY customer_count DESC;