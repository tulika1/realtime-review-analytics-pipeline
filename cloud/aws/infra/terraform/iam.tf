# Least-privilege roles: each compute unit can touch only the prefixes it owns.

data "aws_iam_policy_document" "assume" {
  for_each = {
    firehose  = "firehose.amazonaws.com"
    glue      = "glue.amazonaws.com"
    enrich    = "lambda.amazonaws.com"
    sfn       = "states.amazonaws.com"
    scheduler = "scheduler.amazonaws.com"
  }
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = [each.value]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.assume["firehose"].json
}

resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:ListBucket"]
      Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/bronze/*"]
    }]
  })
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.assume["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_lake" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"],
      Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/bronze/*", "${aws_s3_bucket.lake.arn}/code/*"] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource = ["${aws_s3_bucket.lake.arn}/warehouse/*", "${aws_s3_bucket.lake.arn}/quarantine/*"] },
    ]
  })
}

resource "aws_iam_role" "enrich" {
  name               = "${local.name}-enrich"
  assume_role_policy = data.aws_iam_policy_document.assume["enrich"].json
}

resource "aws_iam_role_policy_attachment" "enrich_logs" {
  role       = aws_iam_role.enrich.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "enrich" {
  role = aws_iam_role.enrich.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Scope further to specific model/inference-profile ARNs once the account's ARNs are known.
      { Effect = "Allow", Action = ["bedrock:InvokeModel", "bedrock-mantle:*"], Resource = "*" },
      { Effect = "Allow", Action = ["dynamodb:GetItem", "dynamodb:PutItem"],
      Resource = aws_dynamodb_table.enrichment_cache.arn },
      { Effect = "Allow", Action = ["s3:PutObject"],
      Resource = ["${aws_s3_bucket.lake.arn}/silver/reviews_enriched_staging/*", "${aws_s3_bucket.lake.arn}/dlq/*"] },
    ]
  })
}

resource "aws_iam_role" "sfn" {
  name               = "${local.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.assume["sfn"].json
}

resource "aws_iam_role_policy" "sfn" {
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:BatchStopJobRun"], Resource = aws_glue_job.silver.arn },
      { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = "arn:aws:lambda:${var.region}:${data.aws_caller_identity.me.account_id}:function:reviewlens-*" },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake.arn}/manifests/*" },
      { Effect = "Allow", Action = ["states:StartExecution", "states:DescribeExecution", "states:StopExecution"], Resource = "*" },
      { Effect = "Allow", Action = ["ecs:RunTask", "ecs:DescribeTasks", "ecs:StopTask", "iam:PassRole"], Resource = "*" },
      { Effect = "Allow", Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"], Resource = "*" },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
    ]
  })
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.assume["scheduler"].json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.pipeline.arn }]
  })
}
