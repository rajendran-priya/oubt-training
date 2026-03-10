import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, unix_timestamp, hour, round, broadcast, when

# 1. Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Ingestion
raw_df = spark.read.parquet("s3://day7-aws-glue-apache-spark/silver-layer/taxi_cleaned")
zones_df = spark.read.csv("s3://day7-aws-glue-apache-spark/bronze-layer/taxi_zone_lookup.csv", header=True, inferSchema=True)

# 3. VALIDATION (Cleaning)
clean_df = raw_df.dropDuplicates().dropna(subset=["PULocationID"])
# Filter out "Impossible" data (Optimization/Validation)
valid_df = clean_df.filter((col("passenger_count") > 0) & (col("total_amount") > 0))

# 4. ENRICHMENT (The Join)
# OPTIMIZATION: Using broadcast() because zones_df is small
enriched_df = valid_df.join(
    broadcast(zones_df), 
    valid_df.PULocationID == zones_df.LocationID, 
    "inner"
)

# 5. FEATURE ENGINEERING (Adding Value)
final_df = enriched_df.withColumn(
    "duration_mins", 
    round((unix_timestamp("dropoff_time") - unix_timestamp("pickup_time")) / 60, 2)
).withColumn("tip_status", when(col("tip_amount") > 0, "Tipped").otherwise("No Tip"))

# 6. OPTIMIZATION (Partitioning)
# Partitioning by Borough makes the data 10x faster for regional reports
output_path = "s3://day7-aws-glue-apache-spark/gold-layer/taxi_final/"
final_df.write.mode("overwrite").partitionBy("Borough").parquet(output_path)

# 7. LOGGING (Validation Report)
print(f"Bronze Count: {raw_df.count()}")
print(f"Final Gold Count: {final_df.count()}")

job.commit()