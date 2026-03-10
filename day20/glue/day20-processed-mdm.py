import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- 1. INITIALIZATION ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

configs = [
    {"domain": "taxi_zone", "path": "s3://day20-demo-oubt/processed/taxi_zone_master_delta/", "key": "LocationID", "name": "Zone"},
    {"domain": "vendor", "path": "s3://day20-demo-oubt/processed/vendor_master_delta/", "key": "vendor_id", "name": "name"},
    {"domain": "ratecode", "path": "s3://day20-demo-oubt/processed/ratecode_master_delta/", "key": "ratecode_id", "name": "description"},
    {"domain": "payment_type", "path": "s3://day20-demo-oubt/processed/payment_type_master_delta/", "key": "payment_type_id", "name": "description"},
]

base_master_path = "s3://day20-demo-oubt/master/"
now_ts = F.current_timestamp()
far_future = F.to_timestamp(F.lit("9999-12-31 23:59:59"))

def stable_hash_expr(key_col: str, name_col: str):
    return F.sha2(
        F.concat_ws("|", F.col(key_col).cast("string"), F.col(name_col).cast("string")),
        256
    )

def drop_if_exists(df, colname: str):
    return df.drop(colname) if colname in df.columns else df

for cfg in configs:
    print(f"Starting MDM SCD2 process for: {cfg['domain']}")
    master_out = f"{base_master_path}{cfg['domain']}_master_governed/"

    # --- 2. LOAD PROCESSED (SILVER) MASTER ---
    src = spark.read.format("delta").load(cfg["path"])

    required_cols = [cfg["key"], cfg["name"], "processed_at"]
    missing = [c for c in required_cols if c not in src.columns]
    if missing:
        raise Exception(f"{cfg['domain']} missing columns: {missing}. Available: {src.columns}")

    # --- 2.1 DROP hash_value IMMEDIATELY (and anywhere else later) ---
    src = drop_if_exists(src, "hash_value")

    # --- 3. SURVIVORSHIP (DE-DUP) ---
    w_survive = Window.partitionBy(cfg["key"]).orderBy(
        F.col("processed_at").desc(),
        F.length(F.col(cfg["name"]).cast("string")).desc(),
    )

    survivor_df = (
        src.withColumn("_rn", F.row_number().over(w_survive))
           .filter(F.col("_rn") == 1)
           .drop("_rn")
    )

    survivor_df = drop_if_exists(survivor_df, "hash_value")

    # --- 4. ENSURE SINGLE HASH COLUMN (row_hash) ---
    survivor_df = survivor_df.withColumn("row_hash", stable_hash_expr(cfg["key"], cfg["name"]))
    survivor_df = drop_if_exists(survivor_df, "hash_value")

    # --- 5. ADD GOVERNANCE / SCD2 COLUMNS ---
    incoming_df = (
        survivor_df
        .withColumn("created_by", F.lit("priya-raj"))
        .withColumn("approved_by", F.lit("data-steward"))
        .withColumn("updated_by", F.lit("priya-raj"))
        .withColumn("is_current", F.lit(True))
        .withColumn("valid_from", now_ts)
        .withColumn("valid_to", far_future)
        .withColumn("created_at", now_ts)
    )

    if "version" not in incoming_df.columns:
        incoming_df = incoming_df.withColumn("version", F.lit(1))

    incoming_df = drop_if_exists(incoming_df, "hash_value")

    # --- 6. CREATE TABLE IF NOT EXISTS ---
    if not DeltaTable.isDeltaTable(spark, master_out):
        incoming_df.write.format("delta").mode("overwrite").save(master_out)
        print(f"Created new governed master table at: {master_out}")
        continue

    tgt = DeltaTable.forPath(spark, master_out)

    # Current rows from target for change detection
    tgt_current = (
        tgt.toDF()
           .filter(F.col("is_current") == True)
           .select(
               F.col(cfg["key"]).alias("t_key"),
               F.col("row_hash").alias("t_hash"),
               F.col("version").alias("t_version")
           )
    )

    # Determine NEW or CHANGED keys
    changes_df = (
        incoming_df.alias("s")
        .join(tgt_current.alias("t"), F.col(f"s.{cfg['key']}") == F.col("t.t_key"), "left")
        .where(F.col("t.t_key").isNull() | (F.col("s.row_hash") != F.col("t.t_hash")))
        .withColumn(
            "version",
            F.when(F.col("t.t_key").isNull(), F.lit(1))
             .otherwise(F.col("t.t_version") + F.lit(1))
        )
        .drop("t_key", "t_hash", "t_version")
    )

    changes_df = drop_if_exists(changes_df, "hash_value")

    if changes_df.rdd.isEmpty():
        print(f"No changes detected for {cfg['domain']}. Skipping merge.")
        continue

    # Expire old current rows
    keys_to_update = changes_df.select(F.col(cfg["key"]).alias("m_key")).distinct()

    (
        tgt.alias("t")
        .merge(keys_to_update.alias("m"), f"t.{cfg['key']} = m.m_key AND t.is_current = true")
        .whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "valid_to": now_ts,
            "updated_by": F.lit("mdm-glue-job"),
        })
        .execute()
    )

    # Insert new current versions
    changes_df.write.format("delta").mode("append").save(master_out)

    print(f"Completed MDM SCD2 for {cfg['domain']}. Updated/Inserted keys: {keys_to_update.count()}. Path: {master_out}")

job.commit()
print("All MDM SCD2 jobs completed successfully.")
