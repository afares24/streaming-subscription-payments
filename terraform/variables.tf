variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "netflix_revenue"
}

variable "environment" {
  description = "Environment (dev/demo/prod)"
  type        = string
  default     = "dev"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (1 shard = ~1k records/sec)"
  type        = number
  default     = 1
  
  validation {
    condition     = var.kinesis_shard_count >= 1 && var.kinesis_shard_count <= 50
    error_message = "Shard count must be between 1 and 50"
  }
}

variable "kinesis_retention_hours" {
  description = "Kinesis data retention in hours"
  type        = number
  default     = 24
}
