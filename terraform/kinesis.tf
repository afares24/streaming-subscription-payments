# Kinesis stream for payment events
resource "aws_kinesis_stream" "payments" {
  name             = "subscription-payments-${var.environment}"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
  ]

  tags = {
    Name = "subscription-payments"
  }
}
