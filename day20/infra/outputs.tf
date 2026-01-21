output "bucket_name" {
  value       = aws_s3_bucket.day20_demo_oubt.bucket
  description = "S3 bucket name"
}

output "lambda_function_arn" {
  value       = aws_lambda_function.validate_raw.arn
  description = "Lambda function ARN"
}

output "sns_topic_arn" {
  value       = aws_sns_topic.day20_pipeline_alerts.arn
  description = "SNS topic ARN for pipeline alerts"
}

output "event_rule_arn" {
  value       = aws_cloudwatch_event_rule.day20_trigger_step_s3.arn
  description = "EventBridge rule ARN for S3 raw trigger"
}

output "state_machine_arn" {
  value       = aws_sfn_state_machine.day20_pipeline.arn
  description = "Step Functions state machine ARN"
}
