import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, unix_timestamp, round

# Initialize Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. LOAD DATASETS ---
# Read the Processed Taxi Data (Delta format)
taxi_df = spark.read.format("delta").load("s3://day20-demo-oubt/processed/yellow_taxi_delta/")

# Read Master Data (Taxi Zones)
# Assuming you have uploaded taxi_zones.csv to your master_data folder
zones_df = spark.read.option("header", "true").csv("s3://day20-demo-oubt/raw/taxi_zone_lookup.csv")

# --- 2. ENRICHMENT & TRANSFORMATION ---
# A. Calculate trip duration in minutes
# B. Join with Zones to get Borough and Zone names instead of IDs
# Note: Renaming LocationID in zones_df to avoid join ambiguity
zones_df = zones_df.withColumnRenamed("LocationID", "zone_id_lookup")

enriched_df = taxi_df \
    .withColumn("trip_duration_min", 
        round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)) \
    .withColumn("trip_revenue", 
        col("fare_amount") + col("tip_amount")) \
    .join(zones_df, taxi_df.PULocationID == zones_df.zone_id_lookup, "left") \
    .withColumnRenamed("Borough", "pickup_borough") \
    .withColumnRenamed("Zone", "pickup_zone") \
    .filter(col("total_amount") > 0) # Basic quality check

# --- 3. WRITE TO CURATED (GOLD) ---Adding partitionBy for better governance
# We use coalesce(1) to ensure the business layer is one clean file
output_path = "s3://day20-demo-oubt/curated/taxi_gold_enriched/"
enriched_df.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_borough") \
    .save(output_path)

job.commit()
print(f"Curated job completed. Data partitioned by borough at {output_path}")