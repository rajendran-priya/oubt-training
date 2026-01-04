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
df = spark.read.format("csv").option("header", "true").load(args['bronze_path'])

# 2. Write to Silver as Delta
print(f"Writing data to {args['silver_path']}...")
df.write.format("delta").mode("overwrite").save(args['silver_path'])

job.commit()