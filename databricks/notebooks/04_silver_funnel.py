# Databricks notebook source

# MAGIC %md
# MAGIC # 04 Silver — Funnel Analytics
# MAGIC
# MAGIC **Purpose:** Computes step-by-step purchase funnel metrics from session data,
# MAGIC producing conversion rates at each stage of the behavioral funnel.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Builds a strict funnel: Page View → Product View → Add to Cart → Purchase
# MAGIC - Computes absolute counts and drop-off rates at each step
# MAGIC - Produces time-to-conversion metrics (how long between funnel steps)
# MAGIC - Slices funnel by time period and product category
# MAGIC - Writes results to `olist.silver.funnel_metrics` and `olist.silver.funnel_step_times`
# MAGIC
# MAGIC **CJA Parallel:** This mirrors the Adobe CJA Fallout and Flow visualizations —
# MAGIC the canonical behavioral analytics view of where users abandon the purchase journey.
# MAGIC
# MAGIC **Inputs:** `olist.silver.sessions`, `olist.silver.session_events`, `olist.bronze.products`
# MAGIC **Outputs:** `olist.silver.funnel_metrics`, `olist.silver.funnel_step_times`

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, sum, count, countDistinct, lit,
    min, max, avg, round as spark_round,
    unix_timestamp, to_date, date_trunc,
    coalesce, percentile_approx
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver Session Data

# COMMAND ----------

sessions = spark.table("olist.silver.sessions")
session_events = spark.table("olist.silver.session_events")

print(f"Sessions loaded: {sessions.count()}")
print(f"Session events loaded: {session_events.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Overall Funnel — Session-Level
# MAGIC
# MAGIC A session "reaches" a funnel step if it contains at least one event of that type.
# MAGIC We use `max_funnel_stage_reached` from the sessions table — this already captures
# MAGIC the deepest step each session progressed to.
# MAGIC
# MAGIC The strict funnel counts how many sessions reached EACH step (not just the max),
# MAGIC which means a session that purchased also counts as having viewed and added to cart.

# COMMAND ----------

total_sessions = sessions.count()

funnel_counts = sessions.agg(
    count("*").alias("total_sessions"),
    sum(when(col("max_funnel_stage_reached") >= 1, 1).otherwise(0)).alias("reached_page_view"),
    sum(when(col("max_funnel_stage_reached") >= 2, 1).otherwise(0)).alias("reached_product_view"),
    sum(when(col("max_funnel_stage_reached") >= 3, 1).otherwise(0)).alias("reached_add_to_cart"),
    sum(when(col("max_funnel_stage_reached") >= 4, 1).otherwise(0)).alias("reached_purchase")
).collect()[0]

# Build a structured funnel metrics DataFrame
from pyspark.sql import Row

funnel_steps = [
    Row(
        funnel_step=1,
        step_name="page_view",
        sessions_entered=int(funnel_counts["reached_page_view"]),
        sessions_total=int(funnel_counts["total_sessions"])
    ),
    Row(
        funnel_step=2,
        step_name="product_view",
        sessions_entered=int(funnel_counts["reached_product_view"]),
        sessions_total=int(funnel_counts["total_sessions"])
    ),
    Row(
        funnel_step=3,
        step_name="add_to_cart",
        sessions_entered=int(funnel_counts["reached_add_to_cart"]),
        sessions_total=int(funnel_counts["total_sessions"])
    ),
    Row(
        funnel_step=4,
        step_name="purchase",
        sessions_entered=int(funnel_counts["reached_purchase"]),
        sessions_total=int(funnel_counts["total_sessions"])
    ),
]

funnel_df = spark.createDataFrame(funnel_steps)

# Add conversion rate columns
from pyspark.sql.functions import lag, round as spark_round

funnel_window = Window.orderBy("funnel_step")

funnel_metrics = funnel_df.withColumn(
    "prev_step_sessions",
    lag("sessions_entered", 1).over(funnel_window)
).withColumn(
    "overall_conversion_rate_pct",
    spark_round(col("sessions_entered") * 100.0 / col("sessions_total"), 2)
).withColumn(
    "step_conversion_rate_pct",
    when(
        col("prev_step_sessions").isNotNull(),
        spark_round(col("sessions_entered") * 100.0 / col("prev_step_sessions"), 2)
    ).otherwise(lit(100.0))
).withColumn(
    "drop_off_rate_pct",
    when(
        col("prev_step_sessions").isNotNull(),
        spark_round(
            (col("prev_step_sessions") - col("sessions_entered")) * 100.0
            / col("prev_step_sessions"), 2
        )
    ).otherwise(lit(0.0))
).drop("prev_step_sessions", "sessions_total")

funnel_metrics.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time-Between-Steps Analysis
# MAGIC
# MAGIC For sessions that progressed through multiple steps, how long (in seconds) did users
# MAGIC spend between each funnel transition? This reveals friction points in the journey.

# COMMAND ----------

# Get first occurrence of each funnel event type per session
from pyspark.sql.functions import first

# We need the timestamp of the first event at each funnel stage per session
step_timestamps = (
    session_events
    .groupBy("session_id", "event_type")
    .agg(min("event_timestamp").alias("first_occurred_at"))
    .groupBy("session_id")
    .pivot("event_type", [
        "web.webpagedetails.pageViews",
        "commerce.productViews",
        "commerce.productListAdds",
        "commerce.purchases"
    ])
    .agg(first("first_occurred_at"))
    .toDF(
        "session_id",
        "ts_page_view",
        "ts_product_view",
        "ts_add_to_cart",
        "ts_purchase"
    )
)

# Compute time deltas between funnel steps (seconds)
step_times = step_timestamps.withColumn(
    "seconds_page_to_product_view",
    when(
        col("ts_page_view").isNotNull() & col("ts_product_view").isNotNull(),
        unix_timestamp("ts_product_view") - unix_timestamp("ts_page_view")
    )
).withColumn(
    "seconds_product_view_to_add",
    when(
        col("ts_product_view").isNotNull() & col("ts_add_to_cart").isNotNull(),
        unix_timestamp("ts_add_to_cart") - unix_timestamp("ts_product_view")
    )
).withColumn(
    "seconds_add_to_purchase",
    when(
        col("ts_add_to_cart").isNotNull() & col("ts_purchase").isNotNull(),
        unix_timestamp("ts_purchase") - unix_timestamp("ts_add_to_cart")
    )
).withColumn(
    "seconds_page_view_to_purchase",
    when(
        col("ts_page_view").isNotNull() & col("ts_purchase").isNotNull(),
        unix_timestamp("ts_purchase") - unix_timestamp("ts_page_view")
    )
)

step_times.show(10, truncate=False)

# COMMAND ----------

# Summary stats for step timing
step_time_summary = step_times.agg(
    spark_round(avg("seconds_page_to_product_view"), 1).alias("avg_sec_page_to_product_view"),
    spark_round(avg("seconds_product_view_to_add"), 1).alias("avg_sec_product_view_to_add"),
    spark_round(avg("seconds_add_to_purchase"), 1).alias("avg_sec_add_to_purchase"),
    spark_round(avg("seconds_page_view_to_purchase"), 1).alias("avg_sec_full_journey"),

    percentile_approx("seconds_page_view_to_purchase", 0.5).alias("median_sec_full_journey"),
    percentile_approx("seconds_page_view_to_purchase", 0.9).alias("p90_sec_full_journey"),
    count(when(col("ts_purchase").isNotNull(), 1)).alias("converting_sessions_with_timestamps")
)
step_time_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funnel by Session Date (Trend View)
# MAGIC
# MAGIC Slice the funnel by day to show conversion rate trends over time.
# MAGIC In a production pipeline, this would power a time-series dashboard.

# COMMAND ----------

daily_funnel = (
    sessions
    .withColumn("session_date", to_date("session_start"))
    .groupBy("session_date")
    .agg(
        count("*").alias("total_sessions"),
        sum(when(col("max_funnel_stage_reached") >= 2, 1).otherwise(0)).alias("product_views"),
        sum(when(col("max_funnel_stage_reached") >= 3, 1).otherwise(0)).alias("add_to_carts"),
        sum(when(col("max_funnel_stage_reached") >= 4, 1).otherwise(0)).alias("purchases"),
    )
    .withColumn(
        "daily_conversion_rate_pct",
        spark_round(col("purchases") * 100.0 / col("total_sessions"), 2)
    )
    .orderBy("session_date")
)

daily_funnel.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Silver Tables

# COMMAND ----------

(
    funnel_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.funnel_metrics")
)
print("✅ olist.silver.funnel_metrics written")

# COMMAND ----------

(
    step_times.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.funnel_step_times")
)
print("✅ olist.silver.funnel_step_times written")

# COMMAND ----------

(
    daily_funnel.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.funnel_daily")
)
print("✅ olist.silver.funnel_daily written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full funnel with conversion and drop-off rates
# MAGIC SELECT
# MAGIC     funnel_step,
# MAGIC     step_name,
# MAGIC     sessions_entered,
# MAGIC     overall_conversion_rate_pct,
# MAGIC     step_conversion_rate_pct,
# MAGIC     drop_off_rate_pct
# MAGIC FROM olist.silver.funnel_metrics
# MAGIC ORDER BY funnel_step;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average time between funnel steps for converting sessions
# MAGIC SELECT
# MAGIC     avg_sec_page_to_product_view,
# MAGIC     avg_sec_product_view_to_add,
# MAGIC     avg_sec_add_to_purchase,
# MAGIC     avg_sec_full_journey,
# MAGIC     median_sec_full_journey,
# MAGIC     p90_sec_full_journey,
# MAGIC     converting_sessions_with_timestamps
# MAGIC FROM olist.silver.funnel_step_times
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily conversion trend
# MAGIC SELECT
# MAGIC     session_date,
# MAGIC     total_sessions,
# MAGIC     purchases,
# MAGIC     daily_conversion_rate_pct
# MAGIC FROM olist.silver.funnel_daily
# MAGIC ORDER BY session_date;