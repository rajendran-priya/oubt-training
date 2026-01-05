
import os
import sys

# Set JAVA_HOME so Spark can find the Java engine
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@17/17.0.17/libexec/openjdk.jdk/Contents/Home"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, when, avg, sum, count, desc, round

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxi_Final_Pipeline") \
    .getOrCreate()

# 2. Load Datasets
# Main taxi data
raw_df = spark.read.parquet("/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/yellow-taxi-data-set /yellow_tripdata_2025-08.parquet")
# Zone lookup table
zones_df = spark.read.csv("/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/yellow-taxi-data-set /taxi_zone_lookup.csv", header=True, inferSchema=True)

# 3. Basic Cleaning (Silver Layer)
# We remove duplicates and invalid records first to keep the Join efficient
clean_df = raw_df.dropDuplicates() \
    .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]) \
    .filter(
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0) & 
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )

# 4. The Join (Adding Names to IDs)
# Joining early so features can be calculated based on Borough if needed
enriched_df = clean_df.join(
    zones_df, 
    clean_df.PULocationID == zones_df.LocationID, 
    "inner"
)

# 5. Feature Engineering (Your logic + Clean naming)
silver_df = enriched_df.withColumn(
    "duration_mins", 
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
).withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
 .withColumn("day_of_week", dayofweek("tpep_pickup_datetime")) \
 .withColumn("tip_status", when(col("tip_amount") > 0, "Tipped").otherwise("No Tip")) \
 .withColumnRenamed("tpep_pickup_datetime", "pickup_time") \
 .withColumnRenamed("tpep_dropoff_datetime", "dropoff_time")

# 6. Gold Layer: Aggregation
# Let's see the average trip duration and total trips per Borough
borough_analysis = silver_df.groupBy("Borough").agg(
    count("*").alias("total_trips"),
    avg("duration_mins").alias("avg_duration_mins"),
    avg("tip_amount").alias("avg_tip_amt")
).orderBy(desc("total_trips"))

# 7. Output Results
print("Sample of Processed Silver Data:")
silver_df.select("pickup_time", "Borough", "duration_mins", "tip_status").show(5)

print("Borough Performance Analysis:")
borough_analysis.show()

# Keep UI alive for a few minutes
import time
print("Spark UI is live at http://localhost:4040")
time.sleep(600)