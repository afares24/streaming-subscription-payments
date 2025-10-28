# Infrastructure outputs
output "s3_bucket_name" {
  description = "S3 lakehouse bucket name"
  value       = aws_s3_bucket.lakehouse.id
}

output "kinesis_stream_name" {
  description = "Kinesis stream name"
  value       = aws_kinesis_stream.payments.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.payments.arn
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.raw.name
}

output "glue_job_name" {
  description = "Glue streaming job name"
  value       = aws_glue_job.streaming.name
}

output "iam_role_arn" {
  description = "IAM role ARN for Glue job"
  value       = aws_iam_role.glue_job.arn
}

# Helper commands
output "start_glue_job_command" {
  description = "Command to start the Glue streaming job"
  value       = "aws glue start-job-run --job-name ${aws_glue_job.streaming.name} --region ${var.aws_region}"
}

output "scale_kinesis_to_demo_command" {
  description = "Command to scale Kinesis to 15 shards for demo"
  value       = "terraform apply -var='kinesis_shard_count=15' -auto-approve"
}

output "scale_kinesis_to_low_cost_command" {
  description = "Command to scale Kinesis back to 1 shard (low cost)"
  value       = "terraform apply -var='kinesis_shard_count=1' -auto-approve"
}

output "athena_workgroup_name" {
  description = "Athena workgroup name for Iceberg queries"
  value       = aws_athena_workgroup.iceberg.name
}
