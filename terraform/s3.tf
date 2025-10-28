# S3 bucket for data lakehouse
resource "aws_s3_bucket" "lakehouse" {
  bucket = "netflix-revenue-lakehouse-${var.environment}"  # Keep hyphens for S3
  
  tags = {
    Name = "${var.project_name}-lakehouse"
  }
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload Glue job scripts
resource "aws_s3_object" "job_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/job.py"
  source = "${path.module}/../glue/job.py"
  etag   = filemd5("${path.module}/../glue/job.py")
}

resource "aws_s3_object" "modules_zip" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/modules.zip"
  source = "${path.module}/../glue/modules.zip"
  etag   = filemd5("${path.module}/../glue/modules.zip")
}
