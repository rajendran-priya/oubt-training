import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# =========================
# DQ SCORECARD GLUE JOB
# Writes one row per check per run to:
#   s3://day20-demo-oubt/curated/dq_scorecard/
# Publishes CloudWatch governance metrics
# =========================

# --- 1) INIT ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# CloudWatch client
cw = boto3.client("cloudwatch")

# --- 2) CONFIG ---
DATASET_NAME = "taxi_gold_enriched"
CURATED_PATH = "s3://day20-demo-oubt/curated/taxi_gold_enriched/"
DQ_OUT_PATH  = "s3://day20-demo-oubt/curated/dq_scorecard/"
CW_NAMESPACE = "Day20/Governance"

# Thresholds
TH_NULL_PCT_BOROUGH = 1.0
TH_NULL_PCT_ZONE    = 1.0
TH_UNKNOWN_PCT      = 5.0
TH_TOTAL_ROWS       = 0.0
TH_NEG_DURATION     = 0.0
TH_MIN_REVENUE      = 0.0

# --- 3) READ CURATED DATA ---
df = spark.read.format("delta").load(CURATED_PATH)

# --- 4) AGGREGATIONS ---
run_ts_col   = F.current_timestamp()
run_date_col = F.date_format(F.current_date(), "yyyy-MM-dd")

agg = df.agg(
    F.count(F.lit(1)).alias("total_rows"),
    F.sum(F.when(F.col("pickup_borough").isNull() | (F.trim(F.col("pickup_borough")) == ""), 1).otherwise(0)).alias("null_pickup_borough_cnt"),
    F.sum(F.when(F.col("pickup_zone").isNull() | (F.trim(F.col("pickup_zone")) == ""), 1).otherwise(0)).alias("null_pickup_zone_cnt"),
    F.sum(F.when(F.col("trip_duration_min") < 0, 1).otherwise(0)).alias("negative_trip_duration_cnt"),
    F.min(F.col("trip_revenue")).alias("min_trip_revenue"),
    F.sum(F.when(F.lower(F.col("pickup_borough")) == "unknown", 1).otherwise(0)).alias("unknown_borough_cnt"),
).collect()[0]

total_rows = float(agg["total_rows"]) if agg["total_rows"] else 0.0

null_borough_pct = (agg["null_pickup_borough_cnt"] * 100.0 / total_rows) if total_rows > 0 else 100.0
null_zone_pct    = (agg["null_pickup_zone_cnt"] * 100.0 / total_rows) if total_rows > 0 else 100.0
unknown_pct      = (agg["unknown_borough_cnt"] * 100.0 / total_rows) if total_rows > 0 else 100.0
neg_duration_cnt = float(agg["negative_trip_duration_cnt"]) if agg["negative_trip_duration_cnt"] else 0.0
min_revenue      = float(agg["min_trip_revenue"]) if agg["min_trip_revenue"] else -1.0

# --- 5) BUILD SCORECARD ---
rows = [
    ("total_rows_gt_0", total_rows, TH_TOTAL_ROWS, "PASS" if total_rows > TH_TOTAL_ROWS else "FAIL"),
    ("null_pickup_borough_pct_lt_1", null_borough_pct, TH_NULL_PCT_BOROUGH, "PASS" if null_borough_pct < TH_NULL_PCT_BOROUGH else "FAIL"),
    ("null_pickup_zone_pct_lt_1", null_zone_pct, TH_NULL_PCT_ZONE, "PASS" if null_zone_pct < TH_NULL_PCT_ZONE else "FAIL"),
    ("negative_trip_duration_eq_0", neg_duration_cnt, TH_NEG_DURATION, "PASS" if neg_duration_cnt == TH_NEG_DURATION else "FAIL"),
    ("min_trip_revenue_gt_0", min_revenue, TH_MIN_REVENUE, "PASS" if min_revenue > TH_MIN_REVENUE else "FAIL"),
    ("unknown_borough_pct_lt_5", unknown_pct, TH_UNKNOWN_PCT, "PASS" if unknown_pct < TH_UNKNOWN_PCT else "FAIL"),
]

scorecard_df = (
    spark.createDataFrame(rows, ["check_name", "metric_value", "threshold", "status"])
    .withColumn("run_ts", run_ts_col)
    .withColumn("dataset", F.lit(DATASET_NAME))
    .withColumn("run_date", run_date_col)
    .select("run_ts", "dataset", "check_name", "metric_value", "threshold", "status", "run_date")
)

# --- 6) WRITE SCORECARD TO S3 ---
scorecard_df.write.mode("append") \
    .format("parquet") \
    .partitionBy("run_date") \
    .save(DQ_OUT_PATH)

print(f"DQ scorecard written to {DQ_OUT_PATH}")
scorecard_df.show(truncate=False)

# --- 7) PUBLISH CLOUDWATCH METRICS ---
total_checks = len(rows)
failed_checks = sum(1 for r in rows if r[3] == "FAIL")
passed_checks = total_checks - failed_checks
dq_pass_rate = (passed_checks * 100.0 / total_checks) if total_checks > 0 else 0.0

dimensions = [{"Name": "Dataset", "Value": DATASET_NAME}]

metric_payload = [
    {"MetricName": "DQ_PassRate", "Dimensions": dimensions, "Value": dq_pass_rate, "Unit": "Percent"},
    {"MetricName": "DQ_FailedChecks", "Dimensions": dimensions, "Value": float(failed_checks), "Unit": "Count"},
    {"MetricName": "UnknownBoroughPct", "Dimensions": dimensions, "Value": float(unknown_pct), "Unit": "Percent"},
    {"MetricName": "NegativeTripDurationCnt", "Dimensions": dimensions, "Value": float(neg_duration_cnt), "Unit": "Count"},
]

try:
    cw.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=metric_payload
    )
    print(f"Published CloudWatch metrics to {CW_NAMESPACE}")
except Exception as e:
    print(f"[WARN] CloudWatch metric publish failed: {e}")

job.commit()
