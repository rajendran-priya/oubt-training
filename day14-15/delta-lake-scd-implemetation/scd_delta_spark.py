"""
scd_delta_spark.py

Final Version: Implements SCD Type 2 with an Audit-Friendly Rollback.
Instead of physical 'RESTORE', it appends the old state as a new version.
"""
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col, coalesce

def get_spark(app_name="scd-delta-demo"):
    builder = SparkSession.builder.appName(app_name)
    # Using the stable configuration verified for your environment
    delta_pkg = 'io.delta:delta-core_2.12:2.1.0'
    builder = builder.config("spark.jars.packages", delta_pkg) \
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return builder.getOrCreate()

def merge_staging_to_delta(spark, staging_df, delta_path, reason='ingest'):
    if not DeltaTable.isDeltaTable(spark, delta_path):
        print(f"--- Initializing Table at {delta_path} ---")
        staging_df.withColumn('version_number', lit(1)) \
                  .withColumn('is_current', lit(True)) \
                  .withColumn('effective_to', lit(None).cast("timestamp")) \
                  .withColumn('change_reason', lit(reason)) \
                  .write.format('delta').mode('overwrite').save(delta_path)
    else:
        print(f"--- Merging Changes (Reason: {reason}) ---")
        dt = DeltaTable.forPath(spark, delta_path)
        
        # Calculate next version number
        max_v = dt.toDF().groupBy("customer_id").max("version_number") \
                  .withColumnRenamed("max(version_number)", "max_v")

        enriched = staging_df.join(max_v, "customer_id", "left") \
            .withColumn("version_number", coalesce(col("max_v") + 1, lit(1))) \
            .withColumn('is_current', lit(True)) \
            .withColumn('effective_to', lit(None).cast("timestamp")) \
            .withColumn('change_reason', lit(reason)) \
            .drop("max_v")

        # SCD Type 2 Union Logic: Expire old and Insert new
        updates = enriched.alias("s").join(dt.toDF().alias("t"), "customer_id") \
            .where("t.is_current = true AND (s.name <> t.name OR s.city <> t.city)") \
            .selectExpr("NULL as mergeKey", "s.*")
            
        source = enriched.selectExpr("customer_id as mergeKey", "*").union(updates)

        dt.alias('t').merge(source.alias('s'), "t.customer_id = s.mergeKey AND t.is_current = true") \
            .whenMatchedUpdate(condition="t.name <> s.name OR t.city <> s.city",
                               set={"is_current": "false", "effective_to": "current_timestamp()"}) \
            .whenNotMatchedInsertAll().execute()

def audit_rollback(spark, delta_path, version_to_restore):
    print(f"--- Performing Audit Rollback to Version {version_to_restore} ---")
    # Step 1: Read the historical data you want to bring back
    historical_data = spark.read.format("delta").option("versionAsOf", version_to_restore) \
                           .load(delta_path).filter("is_current = true") \
                           .select("customer_id", "name", "city") # Get business columns only
    
    # Step 2: Use the existing merge function to apply this "old" data as "new" updates
    merge_staging_to_delta(spark, historical_data, delta_path, reason=f"rollback_to_v{version_to_restore}")

if __name__ == '__main__':
    import sys
    csv_file, d_path = sys.argv[1], sys.argv[2]
    spark = get_spark()
    
    # Initial Load
    staging = spark.read.option('header', 'true').csv(csv_file)
    merge_staging_to_delta(spark, staging, d_path)

    # (Optional) If running with update_data.csv next, it will trigger the 3rd row
    
    # Manual trigger of Rollback to see V3 appear in the list
    if "update" in csv_file:
        audit_rollback(spark, d_path, 0)

    print("\n--- FINAL AUDIT TABLE ---")
    spark.read.format("delta").load(d_path).orderBy("customer_id", "version_number").show()
    spark.stop()