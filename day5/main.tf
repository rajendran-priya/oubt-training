# The S3 Bucket
resource "aws_s3_bucket" "my_app_bucket" {
  bucket = var.bucket_name
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.my_app_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Security: Block Public Access
resource "aws_s3_bucket_public_access_block" "security" {
  bucket = aws_s3_bucket.my_app_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}