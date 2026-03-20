python# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Events — Auto Loader Ingestion
# MAGIC Streams XDM-formatted behavioral event JSON files from the Databricks Volume
# MAGIC into the `olist.bronze.events` Delta table using Auto Loader.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType, MapType, BooleanType
)

# COMMAND ----------

# --- Schema Definition ---
# Explicitly defining schema so Auto Loader doesn't have to infer it
# Mirrors our XDM event schema from src/schemas/xdm_event_schema.json

product_list_item_schema = StructType([
    StructField("SKU", StringType()),
    StructField("name", StringType()),
    StructField("priceTotal", DoubleType()),
    StructField("quantity", IntegerType())
])

order_schema = StructType([
    StructField("purchaseID", StringType()),
    StructField("priceTotal", DoubleType()),
    StructField("currencyCode", StringType()),
    StructField("payments", ArrayType(StructType([
        StructField("paymentType", StringType())
    ])))
])

commerce_schema = StructType([
    StructField("productViews", StructType([StructField("value", IntegerType())])),
    StructField("productListAdds", StructType([StructField("value", IntegerType())])),
    StructField("productListRemovals", StructType([StructField("value", IntegerType())])),
    StructField("purchases", StructType([StructField("value", IntegerType())])),
    StructField("order", order_schema)
])

web_session_schema = StructType([
    StructField("ID", StringType())
])

web_page_details_schema = StructType([
    StructField("name", StringType()),
    StructField("URL", StringType())
])

web_referrer_schema = StructType([
    StructField("URL", StringType()),
    StructField("type", StringType())
])

web_schema = StructType([
    StructField("webSession", web_session_schema),
    StructField("webPageDetails", web_page_details_schema),
    StructField("webReferrer", web_referrer_schema)
])

environment_schema = StructType([
    StructField("type", StringType()),
    StructField("operatingSystem", StringType()),
    StructField("deviceCategory", StringType())
])

event_schema = StructType([
    StructField("_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("eventType", StringType()),
    StructField("identityMap", MapType(StringType(), ArrayType(StructType([
        StructField("id", StringType()),
        StructField("primary", BooleanType())
    ])))),
    StructField("environment", environment_schema),
    StructField("web", web_schema),
    StructField("productListItems", ArrayType(product_list_item_schema)),
    StructField("commerce", commerce_schema)
])

# COMMAND ----------

# --- Paths ---
VOLUME_PATH = "/Volumes/olist/default/olist_raw/events"
BRONZE_TABLE = "olist.bronze.events"
CHECKPOINT_PATH = "/Volumes/olist/default/olist_raw/_checkpoints/bronze_events"

# COMMAND ----------

# --- Auto Loader Stream ---
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("multiLine", "true")
    .schema(event_schema)
    .load(VOLUME_PATH)
)

# Add lineage columns
df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_source_file", col("_metadata.file_path"))

# Write to bronze Delta table
(
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

# COMMAND ----------

# --- Verify ---
display(spark.sql("SELECT eventType, COUNT(*) as event_count FROM olist.bronze.events GROUP BY eventType ORDER BY event_count DESC"))