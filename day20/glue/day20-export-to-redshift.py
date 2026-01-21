import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------- INPUT DELTA PATHS --------
gold_path = "s3://day20-demo-oubt/curated/taxi_gold_enriched/"

vendor_path = "s3://day20-demo-oubt/master/vendor_master_governed/"
payment_path = "s3://day20-demo-oubt/master/payment_type_master_governed/"
ratecode_path = "s3://day20-demo-oubt/master/ratecode_master_governed/"
zone_path = "s3://day20-demo-oubt/master/taxi_zone_master_governed/"

# -------- OUTPUT PARQUET EXPORTS FOR REDSHIFT --------
base_out = "s3://day20-demo-oubt/redshift_exports/"

fact_out   = base_out + "fact_taxi_gold_enriched/"
vendor_out = base_out + "dim_vendor/"
pay_out    = base_out + "dim_payment_type/"
rate_out   = base_out + "dim_ratecode/"
zone_out   = base_out + "dim_taxi_zone/"

# 1) FACT (Curated Gold) -> Parquet
fact_df = spark.read.format("delta").load(gold_path)

# Optional: select only columns you want in Redshift to keep table small
# If you want "all columns", comment out the select.
# fact_df = fact_df.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_borough", "pickup_zone",
#                          "trip_duration_min", "trip_revenue", "vendor_id", "payment_type_id", "ratecode_id",
#                          "PULocationID")

fact_df.write.mode("overwrite").parquet(fact_out)

# Helper to export ONLY current dimension rows (SCD2)
def export_dim(delta_path: str, out_path: str):
    df = spark.read.format("delta").load(delta_path)
    if "is_current" in df.columns:
        df = df.filter(F.col("is_current") == True)
    df.write.mode("overwrite").parquet(out_path)

# 2) DIMS (Master Governed) -> Parquet (current only)
export_dim(vendor_path, vendor_out)
export_dim(payment_path, pay_out)
export_dim(ratecode_path, rate_out)
export_dim(zone_path, zone_out)

job.commit()
print("Redshift export complete.")
