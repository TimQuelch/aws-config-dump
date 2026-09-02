terraform {
  required_version = "~> 1"
  required_providers {
    aws = {
      version = "~> 6.62"
    }
    awscc = {
      version = "~> 1.97"
    }
  }
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

variable "source_sns_topic_arn" { type = string }
variable "source_s3_bucket_name" { type = string }
variable "source_kms_key_arn" { type = string }

locals {
  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.region
  account_id = data.aws_caller_identity.current.account_id
  sns_topic  = var.source_sns_topic_arn
}

# use awscc provider because aws provider doesn't support storage class configuration
resource "awscc_s3tables_table_bucket" "this" {
  table_bucket_name = "acd-aws-config"

  storage_class_configuration = {
    storage_class = "INTELLIGENT_TIERING"
  }

  metrics_configuration = {
    status = "Enabled"
  }
}

resource "aws_s3tables_namespace" "this" {
  namespace        = "acd"
  table_bucket_arn = awscc_s3tables_table_bucket.this.table_bucket_arn
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "lambda_permissions" {
  statement {
    sid    = "WriteToTable"
    effect = "Allow"

    resources = [
      awscc_s3tables_table_bucket.this.table_bucket_arn,
      "${awscc_s3tables_table_bucket.this.table_bucket_arn}/table/*"
    ]

    actions = [
      "s3tables:CreateNamespace",
      "s3tables:CreateTable",
      "s3tables:GetNamespace",
      "s3tables:GetTable",
      "s3tables:GetTableBucket",
      "s3tables:GetTableData",
      "s3tables:GetTableMetadataLocation",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
      "s3tables:PutTableData",
      "s3tables:UpdateTableMetadataLocation",
    ]
  }

  statement {
    sid     = "ReadSource"
    actions = ["s3:GetObject"]
    resources = [
      "arn:aws:s3:::${var.source_s3_bucket_name}/*"
    ]
  }

  statement {
    sid       = "DecryptSource"
    actions   = ["kms:Decrypt"]
    resources = [var.source_kms_key_arn]
  }
}

resource "aws_iam_role" "lambda" {
  name               = "acd-aws-config-history"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachments_exclusive" "lambda" {
  role_name = aws_iam_role.lambda.name
  policy_arns = [
    "arn:aws:iam::aws:policy/CloudWatchLambdaInsightsExecutionRolePolicy",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
  ]
}

resource "aws_iam_role_policy" "lambda" {
  role   = aws_iam_role.lambda.name
  policy = data.aws_iam_policy_document.lambda_permissions.json
}

resource "aws_iam_role_policies_exclusive" "lambda" {
  role_name    = aws_iam_role.lambda.name
  policy_names = [aws_iam_role_policy.lambda.name]
}

resource "aws_s3_bucket" "assets" {
  bucket           = "assets-${local.account_id}-${local.region}-an"
  bucket_namespace = "account-regional"
}

locals {
  source_zip = "${path.module}/lambda/lambda.zip"
}

resource "aws_s3_object" "source" {
  source      = local.source_zip
  source_hash = filemd5(local.source_zip)
  bucket      = aws_s3_bucket.assets.bucket
  key         = "source/source.zip"
}

locals {
  batch_size            = 100
  function_timeout      = 180 # Hopefully this is an upper bound
  batch_window_duration = 300 # 300 is SQS max
}

resource "aws_lambda_function" "this" {
  function_name = "acd-aws-config-history"
  role          = aws_iam_role.lambda.arn

  handler          = "main.lambda_handler"
  s3_bucket        = aws_s3_object.source.bucket
  s3_key           = aws_s3_object.source.key
  source_code_hash = aws_s3_object.source.source_hash

  memory_size = 2048
  timeout     = local.function_timeout

  ephemeral_storage {
    size = 4096
  }

  layers = ["arn:aws:lambda:ap-southeast-2:580247275435:layer:LambdaInsightsExtension:66"]

  logging_config {
    log_format            = "JSON"
    application_log_level = "INFO"
  }

  environment {
    variables = {
      TABLE_BUCKET_ARN = awscc_s3tables_table_bucket.this.table_bucket_arn
      TABLE_NAMESPACE  = aws_s3tables_namespace.this.namespace
    }
  }

  runtime = "python3.14"
}

resource "aws_sqs_queue" "queue" {
  name = "acd-aws-config-history"

  receive_wait_time_seconds  = 20 # enable max long polling

  # Lower than the recommended (6 x timeout + batch window) because the operation is idempotent.
  # This reduces latency a little bit if there are any errors due to conflicing transactions
  visibility_timeout_seconds = 2 * local.function_timeout + local.batch_window_duration
}

resource "aws_lambda_event_source_mapping" "sqs_lambda_mapping" {
  event_source_arn = aws_sqs_queue.queue.arn
  function_name    = aws_lambda_function.this.arn

  batch_size                         = local.batch_size
  maximum_batching_window_in_seconds = local.batch_window_duration
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 2 # limit concurrency to minimum, otherwise they conflict on write
  }

  metrics_config {
    metrics = ["EventCount"]
  }

  filter_criteria {
    filter {
      pattern = jsonencode({
        body = {
          messageType = [
            "ConfigurationHistoryDeliveryCompleted",
            "ConfigurationItemChangeNotification",
          ]
        }
      })
    }
  }
}

data "aws_iam_policy_document" "queue_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    actions = ["sqs:SendMessage"]

    # required, but this is only attached to a single queue. Wildcard instead of specifying the ARN
    # prevents annoying mystery changes when the upstream ARN is 'known after apply'
    resources = ["*"]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [var.source_sns_topic_arn]
    }
  }
}

resource "aws_sqs_queue_policy" "this" {
  queue_url = aws_sqs_queue.queue.id
  policy    = data.aws_iam_policy_document.queue_policy.json
}

resource "aws_sns_topic_subscription" "this" {
  topic_arn = var.source_sns_topic_arn
  endpoint  = aws_sqs_queue.queue.arn

  protocol             = "sqs"
  raw_message_delivery = true # don't wrap in SNS json
}
