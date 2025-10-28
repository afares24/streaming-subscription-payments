# Glue catalog database
resource "aws_glue_catalog_database" "raw" {
  name        = "${var.project_name}_raw"
  description = "Raw layer for ${var.project_name} lakehouse"
}

# Glue streaming job
resource "aws_glue_job" "streaming" {
  name              = "${var.project_name}-kinesis-to-iceberg-${var.environment}"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.lakehouse.id}/scripts/job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                    = "true"
    "--enable-spark-ui"                   = "true"
    "--enable-job-insights"               = "true"
    "--enable-glue-datacatalog"           = "true"
    "--enable-continuous-cloudwatch-log"  = "true"
    "--job-language"                      = "python"
    "--extra-py-files"                    = "s3://${aws_s3_bucket.lakehouse.id}/scripts/modules.zip"
    "--datalake-formats"                  = "iceberg"
    "--conf"                              = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.id}/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.kinesis.endpoint=https://kinesis.${var.aws_region}.amazonaws.com --conf spark.sql.iceberg.handle-timestamp-without-timezone=true --conf spark.sql.parquet.compression.codec=zstd --conf write.parquet.compression-codec=zstd"
    
    # Job parameters
    "--stream_name"         = aws_kinesis_stream.payments.name
    "--table_name"          = "${aws_glue_catalog_database.raw.name}.subscription_payments"
    "--schema_name"         = "subscription_payment"
    "--checkpoint_location" = "s3://${aws_s3_bucket.lakehouse.id}/checkpoints/subscription-payments/"
    "--region"              = var.aws_region
    "--starting_position"   = "LATEST"
  }

  tags = {
    Name = "${var.project_name}-streaming-job"
  }
}
