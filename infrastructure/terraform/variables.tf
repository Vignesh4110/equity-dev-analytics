# variables.tf
# Defines all the input variables for our Terraform configuration.
# This keeps sensitive values and configurable settings
# separate from the main infrastructure code.

variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used to prefix all resource names"
  type        = string
  default     = "equity-dev-analytics"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw ingested data"
  type        = string
  default     = "equity-dev-analytics-raw"
}

variable "processed_bucket_name" {
  description = "S3 bucket for processed parquet data"
  type        = string
  default     = "equity-dev-analytics-processed"
}