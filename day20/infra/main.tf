terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Optional: configure remote state backend here if desired

data "aws_caller_identity" "current" {}

locals {
  bucket_name = "day20-demo-oubt"
  quicksight_datasets = {
    taxi_enriched = "c796cda4-c6d1-40fa-97e9-4c351ab41a39"
    dq_scorecard  = "1092b38a-79c6-4c66-b542-1c3adeb72a3b"
  }
  glue_jobs = {
    "day20-quality-score-card" = {
      script = "day20-quality-score-card.py"
      args   = {}
    }
    "day20-export-to-redshift" = {
      script = "day20-export-to-redshift.py"
      args   = {}
    }
    "day20-processed-mdm" = {
      script = "day20-processed-mdm.py"
      args   = {}
    }
    "day20-processed-curated" = {
      script = "day20-processed-curated.py"
      args   = {}
    }
    "day20-raw-processed-dimension-tables" = {
      script = "day20-raw-processed-dimension-tables.py"
      args   = {}
    }
    "day20-raw-processed" = {
      script = "day20-raw-processed.py"
      args   = {}
    }
  }
}

resource "aws_s3_bucket" "day20_demo_oubt" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_public_access_block" "day20_demo_oubt" {
  bucket                  = aws_s3_bucket.day20_demo_oubt.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable EventBridge notifications from S3 bucket
resource "aws_s3_bucket_notification" "day20_demo_oubt" {
  bucket      = aws_s3_bucket.day20_demo_oubt.id
  eventbridge = true
}

# Lambda: day20-lambda-validate-raw-file
resource "aws_lambda_function" "validate_raw" {
  function_name = "day20-lambda-validate-raw-file"
  role          = var.lambda_role_arn
  handler       = var.lambda_handler
  runtime       = var.lambda_runtime

  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)

  # Environment and timeouts can be added as needed
  timeout = 60
}

# Prefix placeholders
resource "aws_s3_object" "prefixes" {
  for_each = toset([
    "athena-results/",
    "curated/",
    "master/",
    "processed/",
    "raw/",
    "redshift_exports/",
    "scripts/"
  ])

  bucket = aws_s3_bucket.day20_demo_oubt.id
  key    = each.key
  source = "${path.module}/.keep"
  etag   = filemd5("${path.module}/.keep")
}

# IAM role for Glue
resource "aws_iam_role" "glue_role" {
  name = "glue-service-role-day20"

  assume_role_policy = jsonencode({
    Version : "2012-10-17",
    Statement : [{
      Effect : "Allow",
      Principal : { Service : "glue.amazonaws.com" },
      Action : "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access-day20"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [{
      Effect : "Allow",
      Action : [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      Resource : [
        "arn:aws:s3:::${local.bucket_name}",
        "arn:aws:s3:::${local.bucket_name}/*"
      ]
    }]
  })
}

# Glue jobs (scripts stored at s3://day20-demo-oubt/scripts/<jobname>.py)
resource "aws_glue_job" "jobs" {
  for_each = local.glue_jobs

  name     = each.key
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${local.bucket_name}/scripts/${each.value.script}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 1

  default_arguments = merge({
    "--enable-metrics"                 = "true",
    "--enable-continuous-cloudwatch-log" = "true",
    "--TempDir"                        = var.glue_temp_dir,
    "--job-language"                   = "python"
  }, each.value.args)
}

# Step Functions state machine IAM role
resource "aws_iam_role" "sfn_role" {
  name = "day20-step-functions-role"

  assume_role_policy = jsonencode({
    Version : "2012-10-17",
    Statement : [{
      Effect : "Allow",
      Principal : { Service : "states.amazonaws.com" },
      Action : "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  name = "day20-step-functions-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [
      {
        Effect : "Allow",
        Action : ["lambda:InvokeFunction"],
        Resource : aws_lambda_function.validate_raw.arn
      },
      {
        Effect : "Allow",
        Action : ["glue:StartJobRun"],
        Resource : [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-raw-processed",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-raw-processed-dimension-tables",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-processed-mdm",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-processed-curated",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-quality-score-card",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/day20-export-to-redshift"
        ]
      },
      {
        Effect : "Allow",
        Action : ["glue:StartCrawler", "glue:GetCrawler"],
        Resource : [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/day20-curated-crawler",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/day20-data-quality"
        ]
      },
      {
        Effect : "Allow",
        Action : [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource : "*"
      },
      {
        Effect : "Allow",
        Action : ["s3:ListBucket"],
        Resource : "arn:aws:s3:::${local.bucket_name}"
      },
      {
        Effect : "Allow",
        Action : ["s3:GetObject", "s3:PutObject"],
        Resource : "arn:aws:s3:::${local.bucket_name}/athena-results/*"
      },
      {
        Effect : "Allow",
        Action : ["quicksight:CreateIngestion"],
        Resource : [
          "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:dataset/${local.quicksight_datasets.taxi_enriched}",
          "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:dataset/${local.quicksight_datasets.dq_scorecard}"
        ]
      }
    ]
  })
}

locals {
  state_machine_definition = jsonencode({
    Comment : "Day20 Pipeline: Validate -> Raw/Dim Processed -> MDM -> Curated -> DQ -> Wait Crawlers -> Athena DQ Gate -> Load to Redshift -> Refresh QuickSight",
    StartAt : "ValidateRawFile",
    States : {
      ValidateRawFile : {
        Type       : "Task",
        Resource   : "arn:aws:states:::lambda:invoke",
        Parameters : {
          FunctionName : aws_lambda_function.validate_raw.arn,
          "Payload.$" : "$"
        },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 10,
          MaxAttempts     : 2,
          BackoffRate     : 2
        }],
        Catch : [{
          ErrorEquals : ["States.ALL"],
          Next        : "FailPipeline"
        }],
        Next : "RawToProcessedFacts"
      },
      RawToProcessedFacts : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-raw-processed" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 2,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "RawToProcessedDimensions"
      },
      RawToProcessedDimensions : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-raw-processed-dimension-tables" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 2,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "ProcessedToMDM"
      },
      ProcessedToMDM : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-processed-mdm" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 2,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "ProcessedToCurated"
      },
      ProcessedToCurated : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-processed-curated" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 2,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "DQScorecard"
      },
      DQScorecard : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-quality-score-card" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 1,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "StartCuratedCrawler"
      },
      StartCuratedCrawler : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:glue:startCrawler",
        Parameters : { Name : "day20-curated-crawler" },
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "WaitCuratedCrawler"
      },
      WaitCuratedCrawler : { Type : "Wait", Seconds : 30, Next : "GetCuratedCrawler" },
      GetCuratedCrawler : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:glue:getCrawler",
        Parameters : { Name : "day20-curated-crawler" },
        ResultPath : "$.curatedCrawler",
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "CuratedCrawlerStateChoice"
      },
      CuratedCrawlerStateChoice : {
        Type    : "Choice",
        Choices : [
          { Variable : "$.curatedCrawler.Crawler.State", StringEquals : "READY", Next : "StartDQCrawler" },
          { Variable : "$.curatedCrawler.Crawler.State", StringEquals : "RUNNING", Next : "WaitCuratedCrawler" },
          { Variable : "$.curatedCrawler.Crawler.State", StringEquals : "STOPPING", Next : "WaitCuratedCrawler" }
        ],
        Default : "FailPipeline"
      },
      StartDQCrawler : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:glue:startCrawler",
        Parameters : { Name : "day20-data-quality" },
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "WaitDQCrawler"
      },
      WaitDQCrawler : { Type : "Wait", Seconds : 30, Next : "GetDQCrawler" },
      GetDQCrawler : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:glue:getCrawler",
        Parameters : { Name : "day20-data-quality" },
        ResultPath : "$.dqCrawler",
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "DQCrawlerStateChoice"
      },
      DQCrawlerStateChoice : {
        Type    : "Choice",
        Choices : [
          { Variable : "$.dqCrawler.Crawler.State", StringEquals : "READY", Next : "RunDQAthenaQuery" },
          { Variable : "$.dqCrawler.Crawler.State", StringEquals : "RUNNING", Next : "WaitDQCrawler" },
          { Variable : "$.dqCrawler.Crawler.State", StringEquals : "STOPPING", Next : "WaitDQCrawler" }
        ],
        Default : "FailPipeline"
      },
      RunDQAthenaQuery : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:athena:startQueryExecution",
        Parameters : {
          QueryString : "SELECT count(*) AS fail_cnt FROM dq_scorecard WHERE status = 'FAIL' AND run_date = CAST(current_date AS varchar)",
          QueryExecutionContext : { Database : "day20-data-quality" },
          ResultConfiguration   : { OutputLocation : "s3://${local.bucket_name}/athena-results/" }
        },
        ResultPath : "$.athena",
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "WaitForDQAthena"
      },
      WaitForDQAthena : { Type : "Wait", Seconds : 10, Next : "GetDQAthenaStatus" },
      GetDQAthenaStatus : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:athena:getQueryExecution",
        Parameters : { "QueryExecutionId.$" : "$.athena.QueryExecutionId" },
        ResultPath : "$.athenaStatus",
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "DQAthenaStatusChoice"
      },
      DQAthenaStatusChoice : {
        Type    : "Choice",
        Choices : [
          { Variable : "$.athenaStatus.QueryExecution.Status.State", StringEquals : "SUCCEEDED", Next : "GetDQAthenaResults" },
          { Variable : "$.athenaStatus.QueryExecution.Status.State", StringEquals : "RUNNING", Next : "WaitForDQAthena" },
          { Variable : "$.athenaStatus.QueryExecution.Status.State", StringEquals : "QUEUED", Next : "WaitForDQAthena" }
        ],
        Default : "FailPipeline"
      },
      GetDQAthenaResults : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:athena:getQueryResults",
        Parameters : { "QueryExecutionId.$" : "$.athena.QueryExecutionId" },
        ResultPath : "$.dqResults",
        Catch      : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next       : "DQFailCheck"
      },
      DQFailCheck : {
        Type    : "Choice",
        Choices : [{
          Variable           : "$.dqResults.ResultSet.Rows[1].Data[0].VarCharValue",
          NumericGreaterThan : 0,
          Next               : "FailPipelineWithDQ"
        }],
        Default : "LoadToRedshift"
      },
      LoadToRedshift : {
        Type       : "Task",
        Resource   : "arn:aws:states:::glue:startJobRun.sync",
        Parameters : { JobName : "day20-export-to-redshift" },
        Retry : [{
          ErrorEquals     : ["States.ALL"],
          IntervalSeconds : 30,
          MaxAttempts     : 1,
          BackoffRate     : 2
        }],
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "RefreshQS_TaxiGoldEnriched"
      },
      RefreshQS_TaxiGoldEnriched : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:quicksight:createIngestion",
        Parameters : {
          AwsAccountId : data.aws_caller_identity.current.account_id,
          DataSetId    : local.quicksight_datasets.taxi_enriched,
          "IngestionId.$" : "States.Format('ingestion-taxi-{}', $$.Execution.Name)"
        },
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "RefreshQS_DQScorecard"
      },
      RefreshQS_DQScorecard : {
        Type       : "Task",
        Resource   : "arn:aws:states:::aws-sdk:quicksight:createIngestion",
        Parameters : {
          AwsAccountId : data.aws_caller_identity.current.account_id,
          DataSetId    : local.quicksight_datasets.dq_scorecard,
          "IngestionId.$" : "States.Format('ingestion-dq-{}', $$.Execution.Name)"
        },
        Catch : [{ ErrorEquals : ["States.ALL"], Next : "FailPipeline" }],
        Next  : "Success"
      },
      FailPipelineWithDQ : {
        Type : "Fail",
        Cause : "Data Quality checks failed (FAIL rows exist in dq_scorecard for today)",
        Error : "DQ_VALIDATION_FAILED"
      },
      Success : { Type : "Succeed" },
      FailPipeline : {
        Type : "Fail",
        Cause : "Pipeline failed",
        Error : "Day20PipelineFailed"
      }
    }
  })
}

resource "aws_sfn_state_machine" "day20_pipeline" {
  name     = "day20-taxi-pipeline-demo"
  role_arn = aws_iam_role.sfn_role.arn

  definition = local.state_machine_definition

  type = "STANDARD"
}

# EventBridge rule to trigger Step Functions when objects land in raw/
resource "aws_cloudwatch_event_rule" "day20_trigger_step_s3" {
  name        = "day20-trigger-step-s3"
  description = "Trigger step function on raw/ object create"

  event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {"name": [local.bucket_name]},
      "object": {"key": [{"prefix": "raw/"}]}
    }
  })
}

resource "aws_iam_role" "day20_trigger_role" {
  name = "day20-trigger-step-s3-role"

  assume_role_policy = jsonencode({
    Version : "2012-10-17",
    Statement : [{
      Effect : "Allow",
      Principal : { Service : "events.amazonaws.com" },
      Action : "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "day20_trigger_policy" {
  name = "day20-trigger-step-s3-policy"
  role = aws_iam_role.day20_trigger_role.id

  policy = jsonencode({
    Version : "2012-10-17",
    Statement : [{
      Effect : "Allow",
      Action : ["states:StartExecution"],
      Resource : aws_sfn_state_machine.day20_pipeline.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "day20_trigger_target" {
  rule      = aws_cloudwatch_event_rule.day20_trigger_step_s3.name
  target_id = "day20-taxi-pipeline-demo"
  arn       = aws_sfn_state_machine.day20_pipeline.arn
  role_arn  = aws_iam_role.day20_trigger_role.arn
}

# SNS topic for pipeline alerts
resource "aws_sns_topic" "day20_pipeline_alerts" {
  name = "day20-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.day20_pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "raajendran.priya@gmail.com"
}
