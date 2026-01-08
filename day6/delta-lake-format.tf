# --- 1. S3 Script Storage ---
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.datalake_buckets["bronze"].id
  key    = "scripts/convert_to_delta.py"
  source = "${path.module}/convert_to_delta.py"
  etag   = filemd5("${path.module}/convert_to_delta.py")
}

# --- 2. The Glue ETL Job ---
resource "aws_glue_job" "bronze_to_silver_delta" {
  name         = "${var.project_name}-bronze-to-silver-delta"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.datalake_buckets["bronze"].bucket}/scripts/convert_to_delta.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats" = "delta"
    "--conf"             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--bronze_path"      = "s3://${aws_s3_bucket.datalake_buckets["bronze"].bucket}/raw_data/"
    "--silver_path"      = "s3://${aws_s3_bucket.datalake_buckets["silver"].bucket}/delta_data/"
  }
}

# --- 3. Silver Delta Lake Crawler ---
resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.datalake_db.name
  name          = "${var.project_name}-silver-delta-crawler"
  role          = aws_iam_role.glue_service_role.arn

  # Forces Glue to create a 'Native' Delta table (Classification: delta)
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  delta_target {
    # Provide the path to the folder containing the _delta_log
    delta_tables              = ["s3://${aws_s3_bucket.datalake_buckets["silver"].bucket}/delta_data"]
    write_manifest            = false
    create_native_delta_table = true
  }

  tags = {
    Environment = var.environment
    Layer       = "Silver"
  }
}