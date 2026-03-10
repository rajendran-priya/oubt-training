import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit

# Initialize Glue Context
# Remember to add '--datalake-formats': 'delta' in your Job Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define your specific file paths and their corresponding table names
#
master_configs = [
    {"path": "s3://day20-demo-oubt/raw/vendor_id.csv", "table": "vendor_master"},
    {"path": "s3://day20-demo-oubt/raw/taxi_zone_lookup.csv", "table": "taxi_zone_master"},
    {"path": "s3://day20-demo-oubt/raw/ratecode_id.csv", "table": "ratecode_master"},
    {"path": "s3://day20-demo-oubt/raw/payment_type.csv", "table": "payment_type_master"}
]

for config in master_configs:
    print(f"Processing: {config['table']} from {config['path']}")
    
    # 1. READ RAW DATA
    df = spark.read.option("header", "true").csv(config['path'])
    
    # 2. DEDUPLICATION & SCHEMA VALIDATION
    # We create a hash of all columns to ensure uniqueness
    deduped_df = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256)) \
                   .dropDuplicates(["row_hash"]) \
                   .withColumn("processed_at", current_timestamp()) \
                   .withColumn("source_file", lit(config['path']))
    
    # 3. WRITE TO PROCESSED LAYER IN DELTA FORMAT
    # Saving to the processed/ folder for downstream Curated layer joins
    output_path = f"s3://day20-demo-oubt/processed/{config['table']}_delta/"
    
    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    
    print(f"Successfully saved {config['table']} to {output_path}")

job.commit()