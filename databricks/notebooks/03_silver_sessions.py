# Databricks notebook source

# MAGIC %md
# MAGIC # 03 Silver — Session Reconstruction
# MAGIC
# MAGIC **Purpose:** Transforms raw XDM-aligned behavioral events from `olist.bronze.events`
# MAGIC into session-level summaries in `olist.silver.sessions`.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Flattens nested XDM struct columns into a workable event-level DataFrame
# MAGIC - Reconstructs session behavior: event sequences, duration, funnel step reached
# MAGIC - Writes enriched session records to the silver Delta table
# MAGIC
# MAGIC **CJA Parallel:** This mirrors Adobe Customer Journey Analytics session stitching —
# MAGIC grouping raw Experience Events into meaningful visit containers with computed metrics.
# MAGIC
# MAGIC **Inputs:** `olist.bronze.events`
# MAGIC **Outputs:** `olist.silver.sessions`, `olist.silver.session_events`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Schema Inspection
# MAGIC
# MAGIC Run this first to confirm actual column paths from Auto Loader ingestion.
# MAGIC Adjust field references in Section 1 if your XDM paths differ.

# COMMAND ----------

events_raw = spark.table("olist.bronze.events")
events_raw.printSchema()

# COMMAND ----------

events_raw.limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Flatten Bronze Events
# MAGIC
# MAGIC XDM events use nested structs. We extract the fields needed for sessionization.
# MAGIC Adjust the column paths below if printSchema() shows different nesting.

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, when, coalesce, lit,
    min, max, count, sum, countDistinct,
    collect_list, sort_array, struct,
    row_number, lag, datediff, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType

# Flatten key XDM fields into a clean event-level DataFrame.
# NOTE: Adjust struct paths (e.g. session.sessionId) to match your actual schema
# as confirmed by printSchema() above.

flat_events = events_raw.select(

    col("_id").alias("event_id"),
    to_timestamp(col("timestamp")).alias("event_timestamp"),
    col("eventType").alias("event_type"),

    col("web.webSession.ID").alias("session_id"),

    col("identityMap")["customerId"].getItem(0)["id"].alias("customer_id"),

    when(col("eventType") == "commerce.productViews", 1).otherwise(0).alias("is_product_view"),
    when(col("eventType") == "commerce.productListAdds", 1).otherwise(0).alias("is_add_to_cart"),
    when(col("eventType") == "commerce.purchases", 1).otherwise(0).alias("is_purchase"),
    when(col("eventType") == "commerce.productListRemovals", 1).otherwise(0).alias("is_remove_from_cart"),
    when(col("eventType") == "web.webpagedetails.pageViews", 1).otherwise(0).alias("is_page_view"),

    get(col("productListItems"), 0)["SKU"].alias("product_id"),
    get(col("productListItems"), 0)["priceTotal"].cast("double").alias("product_price"),
    get(col("productListItems"), 0)["name"].alias("product_name"),

    col("web.webPageDetails.URL").alias("page_url"),

    col("_ingested_at"),
    col("_source_file")
)

print(f"Flat events count: {flat_events.count()}")
flat_events.show(5, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Session-Level Aggregations
# MAGIC
# MAGIC Each row in `olist.silver.sessions` represents one complete user session.
# MAGIC We compute:
# MAGIC - Temporal bounds (start, end, duration)
# MAGIC - Behavioral counts (views, adds, purchases)
# MAGIC - Funnel stage reached (the deepest step in the session)
# MAGIC - Conversion flag

# COMMAND ----------

# Define funnel stage ordering for "deepest step reached" logic
funnel_stage_map = {
    "web.webpagedetails.pageViews": 1,
    "commerce.productViews": 2,
    "commerce.productListAdds": 3,
    "commerce.productListRemovals": 3,  # same level as add (lateral move)
    "commerce.purchases": 4
}

# Register as a Spark SQL map for use in DataFrame expressions
from pyspark.sql.functions import create_map
from itertools import chain

funnel_map_expr = create_map(
    *chain.from_iterable(
        [(lit(k), lit(v)) for k, v in funnel_stage_map.items()]
    )
)

flat_events_staged = flat_events.withColumn(
    "funnel_stage",
    coalesce(funnel_map_expr[col("event_type")], lit(0))
)

# COMMAND ----------

# Session-level aggregation
sessions_df = flat_events_staged.groupBy("session_id", "customer_id").agg(

    min("event_timestamp").alias("session_start"),
    max("event_timestamp").alias("session_end"),

    count("event_id").alias("total_events"),
    sum("is_page_view").alias("page_views"),
    sum("is_product_view").alias("product_views"),
    sum("is_add_to_cart").alias("add_to_cart_events"),
    sum("is_remove_from_cart").alias("remove_from_cart_events"),
    sum("is_purchase").alias("purchases"),

    countDistinct(
        when(col("product_id").isNotNull(), col("product_id"))
    ).alias("unique_products_viewed"),

    max("funnel_stage").alias("max_funnel_stage_reached"),

    # Capture product_ids seen (for journey reconstruction in notebook 05)
    collect_list(
        when(col("product_id").isNotNull(), col("product_id"))
    ).alias("products_interacted"),

    # Revenue — sum only on purchase events
    sum(
        when(col("is_purchase") == 1, coalesce(col("product_price"), lit(0.0)))
    ).alias("session_revenue")

).withColumn(
    "session_duration_seconds",
    unix_timestamp("session_end") - unix_timestamp("session_start")
).withColumn(
    "converted",
    (col("purchases") > 0).cast(BooleanType())
).withColumn(
    "funnel_stage_name",
    when(col("max_funnel_stage_reached") == 4, "purchase")
    .when(col("max_funnel_stage_reached") == 3, "add_to_cart")
    .when(col("max_funnel_stage_reached") == 2, "product_view")
    .when(col("max_funnel_stage_reached") == 1, "page_view")
    .otherwise("unknown")
)

print(f"Session count: {sessions_df.count()}")
sessions_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build Event-Level Sequence Table
# MAGIC
# MAGIC `olist.silver.session_events` preserves the ordered event trail within each session.
# MAGIC This enables sequence mining, path analysis, and next-event prediction (Phase 8).

# COMMAND ----------

session_event_window = Window.partitionBy("session_id").orderBy("event_timestamp")

session_events_df = flat_events_staged.withColumn(
    "event_sequence_num",
    row_number().over(session_event_window)
).withColumn(
    "prev_event_type",
    lag("event_type", 1).over(session_event_window)
).withColumn(
    "seconds_since_prev_event",
    (unix_timestamp("event_timestamp") -
     unix_timestamp(lag("event_timestamp", 1).over(session_event_window)))
).select(
    "session_id",
    "customer_id",
    "event_sequence_num",
    "event_id",
    "event_timestamp",
    "event_type",
    "prev_event_type",
    "seconds_since_prev_event",
    "product_id",
    "product_price",
    "page_url",
    "funnel_stage",
    "_ingested_at"
)

print(f"Session events count: {session_events_df.count()}")
session_events_df.show(10, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Silver Tables

# COMMAND ----------

# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS olist.silver")

# COMMAND ----------

# Write olist.silver.sessions
(
    sessions_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.sessions")
)
print("✅ olist.silver.sessions written")

# COMMAND ----------

# Write olist.silver.session_events
(
    session_events_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.session_events")
)
print("✅ olist.silver.session_events written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Session summary stats
# MAGIC SELECT
# MAGIC     COUNT(*)                                          AS total_sessions,
# MAGIC     COUNT(DISTINCT customer_id)                      AS unique_customers,
# MAGIC     SUM(CAST(converted AS INT))                      AS converting_sessions,
# MAGIC     ROUND(AVG(session_duration_seconds), 1)          AS avg_session_duration_sec,
# MAGIC     ROUND(AVG(total_events), 1)                      AS avg_events_per_session,
# MAGIC     ROUND(AVG(unique_products_viewed), 2)            AS avg_products_viewed,
# MAGIC     ROUND(SUM(CAST(converted AS INT)) * 100.0 /
# MAGIC           COUNT(*), 2)                               AS session_conversion_rate_pct
# MAGIC FROM olist.silver.sessions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sessions by funnel stage reached
# MAGIC SELECT
# MAGIC     funnel_stage_name,
# MAGIC     COUNT(*) AS session_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_sessions
# MAGIC FROM olist.silver.sessions
# MAGIC GROUP BY funnel_stage_name
# MAGIC ORDER BY CASE funnel_stage_name
# MAGIC     WHEN 'purchase'     THEN 4
# MAGIC     WHEN 'add_to_cart'  THEN 3
# MAGIC     WHEN 'product_view' THEN 2
# MAGIC     WHEN 'page_view'    THEN 1
# MAGIC     ELSE 0
# MAGIC END DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top event transitions (what comes after product_view?)
# MAGIC SELECT
# MAGIC     prev_event_type,
# MAGIC     event_type                  AS next_event_type,
# MAGIC     COUNT(*)                    AS transition_count
# MAGIC FROM olist.silver.session_events
# MAGIC WHERE prev_event_type IS NOT NULL
# MAGIC GROUP BY prev_event_type, event_type
# MAGIC ORDER BY transition_count DESC;