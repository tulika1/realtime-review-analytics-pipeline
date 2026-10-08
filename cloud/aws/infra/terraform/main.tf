terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "reviewlens", owner = var.owner, env = var.env, cost_center = "data-platform" }
  }
}

locals {
  name = "reviewlens-${var.env}"
}

# ----------------------------------------------------------------------------- lake storage
resource "aws_s3_bucket" "lake" {
  bucket = "${local.name}-lake-${data.aws_caller_identity.me.account_id}"
}

data "aws_caller_identity" "me" {}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
  }
  rule {
    id     = "quarantine-and-dlq-expiry"
    status = "Enabled"
    filter { prefix = "dlq/" }
    expiration { days = 30 }
  }
}

# ----------------------------------------------------------------------------- ingestion
resource "aws_kinesis_firehose_delivery_stream" "reviews" {
  name        = "${local.name}-reviews"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.lake.arn
    prefix              = "bronze/reviews/ingest_date=!{timestamp:yyyyMMdd}/"
    error_output_prefix = "bronze/_errors/!{firehose:error-output-type}/ingest_date=!{timestamp:yyyyMMdd}/"
    buffering_interval  = 300 # seconds: trades freshness for fewer, larger files
    buffering_size      = 64  # MB
    compression_format  = "GZIP"
  }
}

# ----------------------------------------------------------------------------- catalog + processing
resource "aws_glue_catalog_database" "layers" {
  for_each = toset(["bronze", "silver", "gold", "gold_audit"])
  name     = "${replace(local.name, "-", "_")}_${each.key}"
}

resource "aws_s3_object" "silver_job" {
  bucket = aws_s3_bucket.lake.id
  key    = "code/glue/silver_job.py"
  source = "${path.module}/../../glue/silver_job.py"
  etag   = filemd5("${path.module}/../../glue/silver_job.py")
}

resource "aws_glue_job" "silver" {
  name              = "reviewlens-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2 # autoscaling caps the bill; raise for backfills
  timeout           = 30

  command {
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.silver_job.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--bronze_path"                      = "s3://${aws_s3_bucket.lake.id}/bronze/reviews/"
    "--warehouse"                        = "s3://${aws_s3_bucket.lake.id}/warehouse/"
    "--quarantine_path"                  = "s3://${aws_s3_bucket.lake.id}/quarantine/"
  }
}

# ----------------------------------------------------------------------------- AI enrichment
resource "aws_dynamodb_table" "enrichment_cache" {
  name         = "reviewlens-enrichment-cache"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "cache_key"
  attribute {
    name = "cache_key"
    type = "S"
  }
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
  point_in_time_recovery { enabled = true }
}

resource "aws_lambda_function" "enrich" {
  function_name = "reviewlens-enrich"
  role          = aws_iam_role.enrich.arn
  runtime       = "python3.12"
  handler       = "reviewlens.handlers.handler"
  filename      = var.enrich_lambda_zip # built by `make package`
  timeout       = 300
  memory_size   = 512
  # Hard cap on parallel model calls = cost + rate-limit guard independent of the Map setting.
  reserved_concurrent_executions = 10
  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      CACHE_TABLE      = aws_dynamodb_table.enrichment_cache.name
      REVIEWLENS_MODEL = var.model_id
    }
  }
}

# ----------------------------------------------------------------------------- orchestration
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "oncall" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../orchestration/state_machine.asl.json", {
    bronze_path      = "s3://${aws_s3_bucket.lake.id}/bronze/reviews/"
    warehouse        = "s3://${aws_s3_bucket.lake.id}/warehouse/"
    quarantine_path  = "s3://${aws_s3_bucket.lake.id}/quarantine/"
    lake_bucket      = aws_s3_bucket.lake.id
    alerts_topic_arn = aws_sns_topic.alerts.arn
  })
}

resource "aws_scheduler_schedule" "every_15m" {
  name                = "${local.name}-every-15m"
  schedule_expression = "rate(15 minutes)"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ ingest_date = "<aws.scheduler.scheduled-time>" })
  }
}

# ----------------------------------------------------------------------------- cost guardrail
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$reviewlens"]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
