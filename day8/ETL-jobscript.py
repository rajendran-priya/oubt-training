import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import re

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Load Data Sources
raw_data = glueContext.create_dynamic_frame.from_catalog(
    database="day8-taxi-db", 
    table_name="raw_data"
)

master_data = glueContext.create_dynamic_frame.from_catalog(
    database="day8-taxi-db", 
    table_name="master_data"
)

# 2. Define and Run Data Quality Rules
# Includes Completeness, Accuracy, Validity, Consistency, and Anomaly Detection
dq_ruleset = """
    Rules = [
        # 1. Completeness
        IsComplete "vendorid",
        IsComplete "tpep_pickup_datetime",
        IsComplete "tpep_dropoff_datetime",
        
        # 2. Accuracy
        ColumnValues "passenger_count" > 0,
        ColumnValues "trip_distance" >= 0,
        ColumnValues "fare_amount" >= 0,
        ColumnValues "total_amount" between 0 and 1000,
        
        # 3. Validity
        ColumnValues "payment_type" in [1, 2, 3, 4, 5, 6],
        ColumnValues "ratecodeid" in [1, 2, 3, 4, 5, 6, 99],
        
        # 4. Consistency: Integrity check against zones table
        ReferentialIntegrity "pulocationid" "zones.locationid" >= 0.98,
        
        # 5. Anomaly Detection: SQL check for 0 passengers on paid trips
        CustomSql "SELECT COUNT(*) FROM primary WHERE total_amount > 0 AND passenger_count = 0" = 0
    ]
"""

dq_results = EvaluateDataQuality().process_rows(
    frame=raw_data,
    additional_data_sources={"zones": master_data},
    ruleset=dq_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "dq_evaluation",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    },
    additional_options={"performanceTuning.caching":"CACHE_NOTHING"}
)

# 3. Extract Outcomes
row_level_outcomes = SelectFromCollection.apply(dfc=dq_results, key="rowLevelOutcomes")
rule_outcomes = SelectFromCollection.apply(dfc=dq_results, key="ruleOutcomes")

# 4. Split Data (Passed vs Failed)
# Using Spark DataFrame to avoid the "wrong number of arguments" introspection bug
row_df = row_level_outcomes.toDF()

passed_df = row_df.filter(row_df["DataQualityEvaluationResult"] == "Passed")
failed_df = row_df.filter(row_df["DataQualityEvaluationResult"] == "Failed")

# 5. Write to S3 using Spark Parquet Writer
# This bypasses Glue-specific writer errors and cleans hidden metadata

# A. Rule Summary (Transformed Bucket)
# We select only string/simple fields to ensure Parquet compatibility
rule_summary_df = rule_outcomes.toDF().select("Rule", "Outcome", "FailureReason", "EvaluatedRule")
rule_summary_df.write.mode("overwrite").parquet("s3://day8-aws-glue-data-quality/transformed-data/")

# B. Failed Records
# Note: This includes the 'DataQualityEvaluationResult' column for tracking
failed_df.write.mode("overwrite").parquet("s3://day8-aws-glue-data-quality/passed-failed-data-quality-check/failed/")

# C. Passed Records
passed_df.write.mode("overwrite").parquet("s3://day8-aws-glue-data-quality/passed-failed-data-quality-check/passed/")

job.commit()