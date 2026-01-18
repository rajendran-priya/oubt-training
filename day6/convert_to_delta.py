import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get parameters from Terraform
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bronze_path', 'silver_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read the CSV from Bronze
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(args['bronze_path'])

# 2. Write to Silver as Delta
print(f"Writing data to {args['silver_path']}...")

# UPDATED: Added mergeSchema and changed mode to append for versioning demo
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(args['silver_path'])

# 3. Optional: Print History to Logs (Great for Demo Debugging)
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, args['silver_path'])
print("Current Table History:")
delta_table.history().select("version", "timestamp", "operation").show()

job.commit()