# main.tf
# Main Terraform configuration that provisions our AWS infrastructure.
#
# Resources created:
#   - S3 bucket for raw data (JSON files from APIs)
#   - S3 bucket for processed data (Parquet files from Spark)
#   - IAM policy giving our user access to both buckets
#   - Bucket versioning so we can recover deleted files
#   - Lifecycle rules to manage storage costs

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.0"
}

# Configure the AWS provider
# Credentials come from environment variables or ~/.aws/credentials
provider "aws" {
  region = var.aws_region
}

# ── Raw Data Bucket ────────────────────────────────────────────────
# Stores raw JSON files from Alpha Vantage, Polygon, and GitHub APIs
resource "aws_s3_bucket" "raw_data" {
  bucket = var.raw_bucket_name

  tags = {
    Name        = "${var.project_name}-raw"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Enable versioning on raw bucket
# This lets us recover accidentally deleted or overwritten files
resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public access to raw bucket
# Our data is private — never expose it publicly
resource "aws_s3_bucket_public_access_block" "raw_data_public_access" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rule for raw bucket
# Move files to cheaper storage after 90 days
# Delete files after 1 year to manage costs
resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "raw-data-lifecycle"
    status = "Enabled"

    # Filter is required — empty filter means apply to all objects
    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 365
    }
    }
}

# ── Processed Data Bucket ──────────────────────────────────────────
# Stores cleaned Parquet files output by PySpark
resource "aws_s3_bucket" "processed_data" {
  bucket = var.processed_bucket_name

  tags = {
    Name        = "${var.project_name}-processed"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Enable versioning on processed bucket
resource "aws_s3_bucket_versioning" "processed_data_versioning" {
  bucket = aws_s3_bucket.processed_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public access to processed bucket
resource "aws_s3_bucket_public_access_block" "processed_data_public_access" {
  bucket = aws_s3_bucket.processed_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM Policy ─────────────────────────────────────────────────────
# Gives our IAM user permission to read and write to both buckets
# This follows the principle of least privilege —
# only grant the permissions actually needed
resource "aws_iam_policy" "s3_pipeline_policy" {
  name        = "${var.project_name}-s3-policy"
  description = "Allows read/write access to pipeline S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      }
    ]
  })
}

# Attach the policy to our IAM user
resource "aws_iam_user_policy_attachment" "pipeline_user_policy" {
  user       = "equity-dev-analytics-user"
  policy_arn = aws_iam_policy.s3_pipeline_policy.arn
}