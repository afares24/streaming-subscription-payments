# Athena workgroup for Iceberg queries
resource "aws_athena_workgroup" "iceberg" {
  name        = "${var.project_name}-iceberg-${var.environment}"
  description = "Workgroup for Iceberg table operations"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Name = "${var.project_name}-athena-workgroup"
  }
}
