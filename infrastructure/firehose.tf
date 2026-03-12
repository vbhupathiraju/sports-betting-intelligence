# ─────────────────────────────────────────────
# IAM Role for Firehose to write to S3
# ─────────────────────────────────────────────

resource "aws_iam_role" "firehose_role" {
  name = "sports-betting-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose_s3_policy" {
  name = "sports-betting-firehose-s3-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject"
      ]
      Resource = [
        "arn:aws:s3:::sports-betting-raw-data-974482386805",
        "arn:aws:s3:::sports-betting-raw-data-974482386805/*"
      ]
    }]
  })
}

# ─────────────────────────────────────────────
# Firehose: odds
# ─────────────────────────────────────────────

resource "aws_kinesis_firehose_delivery_stream" "odds_stream" {
  name        = "sports-betting-odds-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = "arn:aws:s3:::sports-betting-raw-data-974482386805"
    prefix             = "odds/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "odds-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 5
    buffering_interval  = 300
    compression_format  = "UNCOMPRESSED"
  }
}

# ─────────────────────────────────────────────
# Firehose: kalshi
# ─────────────────────────────────────────────

resource "aws_kinesis_firehose_delivery_stream" "kalshi_stream" {
  name        = "sports-betting-kalshi-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = "arn:aws:s3:::sports-betting-raw-data-974482386805"
    prefix             = "kalshi/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "kalshi-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 5
    buffering_interval  = 300
    compression_format  = "UNCOMPRESSED"
  }
}

# ─────────────────────────────────────────────
# Firehose: game-events
# ─────────────────────────────────────────────

resource "aws_kinesis_firehose_delivery_stream" "game_events_stream" {
  name        = "sports-betting-game-events-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = "arn:aws:s3:::sports-betting-raw-data-974482386805"
    prefix             = "game-events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "game-events-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 5
    buffering_interval  = 300
    compression_format  = "UNCOMPRESSED"
  }
}
