import sys
import boto3
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sha2, concat_ws, col, to_timestamp

# Initialize Clients and Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glue_client = boto3.client('glue')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. CRAWLER AUTOMATION ---
def run_crawler(crawler_name):
    print(f"Starting crawler: {crawler_name}")
    try:
        glue_client.start_crawler(Name=crawler_name)
    except glue_client.exceptions.CrawlerRunningException:
        print(f"Crawler {crawler_name} is already running.")
    
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response['Crawler']['State']
        if status == 'READY':
            print(f"Crawler {crawler_name} is ready.")
            break
        print(f"Crawler {crawler_name} is currently {status}. Waiting...")
        time.sleep(40)

# Trigger the crawler before reading data
run_crawler("day20-final-raw-data")

# --- 2. DATA PROCESSING ---
# Reading from your specific database and table
raw_df = glueContext.create_dynamic_frame.from_catalog(
    database="day20-demo-oubt", 
    table_name="yellow_tripdata_2025_08_parquet" 
).toDF()

# A. Schema Enforcement
processed_df = raw_df \
    .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
    .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime")) \
    .withColumn("fare_amount", col("fare_amount").cast("decimal(10,2)")) \
    .withColumn("tip_amount", col("tip_amount").cast("decimal(10,2)")) \
    .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))

# B. Hash-Based Deduplication (Matching Engine)
# This uses all columns to find exact duplicates and keep only one unique record
deduped_df = processed_df.withColumn(
    "row_hash", 
    sha2(concat_ws("||", *processed_df.columns), 256)
).dropDuplicates(["row_hash"]).drop("row_hash")

# --- 3. WRITE TO S3 (PROCESSED ZONE) ---
# Saving to your designated processed path in Delta format
# FIX: .coalesce(1) ensures only ONE file is created instead of many small ones
output_path = "s3://day20-demo-oubt/processed/yellow_taxi_delta/"
deduped_df.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(output_path)
    
job.commit()
print(f"Job completed. Data saved to {output_path}")