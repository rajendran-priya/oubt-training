output "s3_bucket_arn" {
  description = "The ARN of the bucket (needed later for Lambda permissions)"
  value       = aws_s3_bucket.my_app_bucket.arn
}

output "s3_bucket_name" {
  value = aws_s3_bucket.my_app_bucket.id
}

output "lambda_function_name" {
  value = aws_lambda_function.s3_processor.function_name
}