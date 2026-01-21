variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "lambda_role_arn" {
  description = "IAM role ARN for the Lambda function"
  type        = string
}

variable "lambda_zip" {
  description = "Path to the Lambda deployment package (zip)"
  type        = string
}

variable "lambda_handler" {
  description = "Lambda handler (e.g., index.handler)"
  type        = string
  default     = "index.handler"
}

variable "lambda_runtime" {
  description = "Lambda runtime (e.g., python3.11)"
  type        = string
  default     = "python3.11"
}

variable "glue_temp_dir" {
  description = "S3 location for Glue temporary data"
  type        = string
  default     = "s3://day20-demo-oubt/athena-results/"
}
