import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.functions import col, round, unix_timestamp, hour, dayofweek, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# 1. Source: AWS Glue Data Catalog
AWSGlueDataCatalog_node1768020020449 = glueContext.create_dynamic_frame.from_catalog(
    database="day11-aws-step", 
    table_name="yellow_tripdata_2025_08_parquet", 
    transformation_ctx="AWSGlueDataCatalog_node1768020020449"
)

# 2. FIX: Manual Schema Mapping (Bypassing "object" error)
# We convert to Spark DF and cast types explicitly
input_df = AWSGlueDataCatalog_node1768020020449.toDF()

cleaned_df = input_df.select(
    col("vendorid").cast("int"),
    col("tpep_pickup_datetime").cast("timestamp"),
    col("tpep_dropoff_datetime").cast("timestamp"),
    col("passenger_count").cast("long"),
    col("trip_distance").cast("double"),
    col("ratecodeid").cast("long").alias("rate_code_id"),
    col("store_and_fwd_flag").cast("string"),
    col("pulocationid").cast("int").alias("pick_up_location_id"),
    col("dolocationid").cast("int").alias("dropoff_location_id"),
    col("payment_type").cast("long"),
    col("fare_amount").cast("double"),
    col("extra").cast("double"),
    col("mta_tax").cast("double"),
    col("tip_amount").cast("double"),
    col("tolls_amount").cast("double"),
    col("improvement_surcharge").cast("double"),
    col("total_amount").cast("double"),
    col("congestion_surcharge").cast("double"),
    col("airport_fee").cast("double"),
    col("cbd_congestion_fee").cast("double")
)

# Convert back to DynamicFrame for Glue transforms
ChangeSchema_node1768020233383 = DynamicFrame.fromDF(cleaned_df, glueContext, "ChangeSchema_node1768020233383")

# 3. Filter Node
Filter_node1768020328605 = Filter.apply(
    frame=ChangeSchema_node1768020233383, 
    f=lambda row: (row["trip_distance"] > 0 and row["fare_amount"] >= 0 and row["passenger_count"] > 0 and row["rate_code_id"] >= 1 and row["rate_code_id"] <= 6), 
    transformation_ctx="Filter_node1768020328605"
)

# 4. Drop Duplicates
DropDuplicates_node1768026361778 = DynamicFrame.fromDF(
    Filter_node1768020328605.toDF().dropDuplicates(), 
    glueContext, 
    "DropDuplicates_node1768026361778"
)

# 5. Derived Columns (Combined into one Spark SQL operation for efficiency)
final_spark_df = DropDuplicates_node1768026361778.toDF().withColumn(
    "duration_mins", 
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
).withColumn(
    "pickup_hour", hour(col("tpep_pickup_datetime"))
).withColumn(
    "day_of_week", dayofweek(col("tpep_pickup_datetime"))
).withColumn(
    "ingestion_timestamp", current_timestamp()
)

DerivedColumn_final = DynamicFrame.fromDF(final_spark_df, glueContext, "DerivedColumn_final")

# 6. Data Quality and S3 Target
EvaluateDataQuality().process_rows(
    frame=DerivedColumn_final, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

AmazonS3_node1768027986401 = glueContext.write_dynamic_frame.from_options(
    frame=DerivedColumn_final, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": "s3://day11-step-functions/transformed-data/", "partitionKeys": []}, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_node1768027986401"
)

job.commit()