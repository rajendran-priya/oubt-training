# --- 1. Provider & Variables ---
provider "aws" {
  region = "us-east-1"
}

variable "project_name" {
  default = "day5-medallion-datalake"
}

variable "environment" {
  default = "prod"
}

# --- 2. S3 Zonal Infrastructure ---
locals {
  # We renamed 'raw' to 'bronze' here. 
  # 'each.key' will now pick up these new names.
  zones = {
    bronze = "Confidential"
    silver = "Internal"
    gold   = "Public"
  }
}

resource "aws_s3_bucket" "datalake_buckets" {
  for_each = local.zones
  bucket   = "${var.project_name}-${each.key}-${var.environment}"

  tags = {
    DataZone           = each.key   # This will be "bronze", "silver", or "gold"
    DataClassification = each.value # This will be "Confidential",Internal or Public
    Environment        = var.environment
  }
}

# Force Private Access
resource "aws_s3_bucket_public_access_block" "block_all" {
  for_each = aws_s3_bucket.datalake_buckets
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enforce Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  for_each = aws_s3_bucket.datalake_buckets
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# --- 3. IAM Role for Glue Crawler ---
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "GlueS3Access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [for b in aws_s3_bucket.datalake_buckets : "${b.arn}/*"]
    }]
  })
}

# --- 4. Data Catalog & Crawler ---
resource "aws_glue_catalog_database" "datalake_db" {
  name = "${var.project_name}_catalog"
}

resource "aws_glue_crawler" "bronze_crawler" {
  database_name = aws_glue_catalog_database.datalake_db.name
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue_service_role.arn

  s3_target {
    # This now dynamically points to the 'bronze' bucket
    path = "s3://${aws_s3_bucket.datalake_buckets["bronze"].bucket}/"
  }
}