# Databricks notebook source
# NYC Taxi Data Engineering Pipeline
# Configuration

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_PATH = "/Volumes/workspace/default/nyc_taxi_data/yellow_tripdata_2026-01.parquet"

SILVER_PATH = "/Volumes/workspace/default/nyc_taxi_data/silver"
GOLD_DAILY_PATH = "/Volumes/workspace/default/nyc_taxi_data/gold_daily"
GOLD_HOURLY_PATH = "/Volumes/workspace/default/nyc_taxi_data/gold_hourly"
GOLD_LOCATION_PATH = "/Volumes/workspace/default/nyc_taxi_data/gold_location"
GOLD_PEAK_PATH = "/Volumes/workspace/default/nyc_taxi_data/gold_peak_hours"
GOLD_INSIGHTS_PATH = "/Volumes/workspace/default/nyc_taxi_data/gold_insights"

START_DATE = "2026-01-01"
END_DATE = "2026-01-31"

print("NYC Taxi PySpark Pipeline")
print(f"Processing period: {START_DATE} to {END_DATE}")

# COMMAND ----------

# COMMAND ----------

# ============================================================
# STEP 1: BRONZE / RAW INGESTION
# ============================================================

df = spark.read.parquet(SOURCE_PATH)

print("Bronze ingestion complete")
print("Rows:", df.count())
print("Columns:", len(df.columns))

display(df.limit(10))

# COMMAND ----------

# ============================================================
# STEP 2: BRONZE DATA PROFILING
# ============================================================

bronze_quality = df.select(
    F.count("*").alias("total_rows"),
    F.sum(F.col("tpep_pickup_datetime").isNull().cast("int")).alias("null_pickup"),
    F.sum(F.col("tpep_dropoff_datetime").isNull().cast("int")).alias("null_dropoff"),
    F.sum((F.col("passenger_count") <= 0).cast("int")).alias("invalid_passengers"),
    F.sum((F.col("trip_distance") <= 0).cast("int")).alias("invalid_distance"),
    F.sum((F.col("fare_amount") < 0).cast("int")).alias("negative_fares"),
    F.sum((F.col("total_amount") < 0).cast("int")).alias("negative_total")
)

display(bronze_quality)

# COMMAND ----------

# ============================================================
# STEP 3: SILVER CLEANING AND TRANSFORMATION
# ============================================================

silver_df = (
    df
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("total_amount") >= 0)
    .withColumn(
        "trip_duration_minutes",
        F.timestamp_diff(
            F.lit("SECOND"),
            F.col("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime")
        ) / 60.0
    )
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )
    .withColumn(
        "revenue_per_mile",
        F.when(
            F.col("trip_distance") > 0,
            F.col("total_amount") / F.col("trip_distance")
        )
    )
    .withColumn(
        "tip_percentage",
        F.when(
            F.col("fare_amount") > 0,
            (F.col("tip_amount") / F.col("fare_amount")) * 100
        )
    )
)

print("Silver transformation complete")
print("Silver rows:", silver_df.count())
print("Silver columns:", len(silver_df.columns))

# COMMAND ----------

# ============================================================
# STEP 4: SAVE SILVER DELTA LAYER
# ============================================================

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print("Silver Delta layer saved successfully")
print("Path:", SILVER_PATH)

# COMMAND ----------

# ============================================================
# STEP 5: GOLD DAILY PERFORMANCE
# ============================================================

gold_daily = (
    silver_df
    .filter(
        (F.col("pickup_date") >= F.lit(START_DATE)) &
        (F.col("pickup_date") <= F.lit(END_DATE))
    )
    .groupBy("pickup_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare_revenue"),
        F.round(F.sum("tip_amount"), 2).alias("total_tip_revenue"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage"),
        F.round(F.avg("revenue_per_mile"), 2).alias("avg_revenue_per_mile")
    )
    .orderBy("pickup_date")
)

print("Gold Daily rows:", gold_daily.count())

display(gold_daily)

# COMMAND ----------

# ============================================================
# STEP 6: SAVE GOLD DAILY DELTA
# ============================================================

gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_DAILY_PATH)

print("Gold Daily Delta layer saved successfully")
print("Path:", GOLD_DAILY_PATH)

# COMMAND ----------

# ============================================================
# STEP 7: GOLD HOURLY PERFORMANCE
# ============================================================

gold_hourly = (
    silver_df
    .filter(
        (F.col("pickup_date") >= F.lit(START_DATE)) &
        (F.col("pickup_date") <= F.lit(END_DATE))
    )
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage")
    )
    .orderBy("pickup_hour")
)

print("Gold Hourly rows:", gold_hourly.count())

display(gold_hourly)

# COMMAND ----------

# ============================================================
# STEP 8: SAVE GOLD HOURLY DELTA
# ============================================================

gold_hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_HOURLY_PATH)

print("Gold Hourly Delta layer saved successfully")
print("Path:", GOLD_HOURLY_PATH)

# COMMAND ----------

# ============================================================
# STEP 9: GOLD LOCATION PERFORMANCE
# ============================================================

gold_location = (
    silver_df
    .filter(
        (F.col("pickup_date") >= F.lit(START_DATE)) &
        (F.col("pickup_date") <= F.lit(END_DATE))
    )
    .groupBy("PULocationID")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage")
    )
    .withColumnRenamed("PULocationID", "pickup_location_id")
    .orderBy(F.desc("total_trips"))
)

print("Gold Location rows:", gold_location.count())

display(gold_location.limit(20))

# COMMAND ----------

# ============================================================
# STEP 10: SAVE GOLD LOCATION DELTA
# ============================================================

gold_location.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_LOCATION_PATH)

print("Gold Location Delta layer saved successfully")
print("Path:", GOLD_LOCATION_PATH)

# COMMAND ----------

# ============================================================
# STEP 11: PEAK HOUR RANKING
# ============================================================

hour_window = Window.orderBy(F.desc("total_trips"))

gold_peak_hours = (
    gold_hourly
    .withColumn(
        "demand_rank",
        F.row_number().over(hour_window)
    )
    .orderBy("demand_rank")
)

print("Peak Hours rows:", gold_peak_hours.count())

display(gold_peak_hours)

# COMMAND ----------

# ============================================================
# STEP 12: SAVE GOLD PEAK HOURS DELTA
# ============================================================

gold_peak_hours.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PEAK_PATH)

print("Gold Peak Hours Delta layer saved successfully")
print("Path:", GOLD_PEAK_PATH)

# COMMAND ----------

# ============================================================
# STEP 13: FINAL BUSINESS INSIGHTS
# ============================================================

daily_summary = gold_daily.agg(
    F.sum("total_trips").alias("total_trips"),
    F.sum("total_passengers").alias("total_passengers"),
    F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
    F.round(F.avg("avg_trip_distance"), 2).alias("avg_trip_distance"),
    F.round(F.avg("avg_trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
    F.round(F.avg("avg_tip_percentage"), 2).alias("avg_tip_percentage")
)

peak_hour = (
    gold_peak_hours
    .filter(F.col("demand_rank") == 1)
    .select(
        "pickup_hour",
        "total_trips",
        "total_revenue",
        "avg_fare"
    )
)

top_location = (
    gold_location
    .orderBy(F.desc("total_trips"))
    .limit(1)
    .select(
        "pickup_location_id",
        "total_trips",
        "total_revenue",
        "avg_fare"
    )
)

print("===== JANUARY 2026 SUMMARY =====")
display(daily_summary)

print("===== PEAK DEMAND HOUR =====")
display(peak_hour)

print("===== TOP PICKUP LOCATION =====")
display(top_location)

# COMMAND ----------

# ============================================================
# STEP 14: SAVE FINAL GOLD INSIGHTS
# ============================================================

daily_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_INSIGHTS_PATH)

print("Gold Insights Delta layer saved successfully")
print("Path:", GOLD_INSIGHTS_PATH)

# COMMAND ----------

# ============================================================
# STEP 15: FINAL PIPELINE VALIDATION
# ============================================================

print("===== PIPELINE VALIDATION =====")

print("\nSilver:", spark.read.format("delta").load(SILVER_PATH).count())
print("Gold Daily:", spark.read.format("delta").load(GOLD_DAILY_PATH).count())
print("Gold Hourly:", spark.read.format("delta").load(GOLD_HOURLY_PATH).count())
print("Gold Location:", spark.read.format("delta").load(GOLD_LOCATION_PATH).count())
print("Gold Peak Hours:", spark.read.format("delta").load(GOLD_PEAK_PATH).count())
print("Gold Insights:", spark.read.format("delta").load(GOLD_INSIGHTS_PATH).count())

print("\n===== VALIDATION COMPLETE =====")

# COMMAND ----------
