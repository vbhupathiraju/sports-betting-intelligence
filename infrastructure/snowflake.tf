resource "aws_iam_role" "snowflake_role" {
  name        = "sports-betting-snowflake-role"
  description = "Allows Snowflake to read processed signals from S3"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::088253294712:user/0ywj1000-s"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "CMC08809_SFCRole=6_0Scy1BPQdeL4wPJE77Lc4V/xDCg="
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  name = "sports-betting-snowflake-s3-read"
  role = aws_iam_role.snowflake_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::sports-betting-raw-data-974482386805/processed/*"
      },
      {
        Effect = "Allow"
        Action = "s3:ListBucket"
        Resource = "arn:aws:s3:::sports-betting-raw-data-974482386805"
        Condition = {
          StringLike = {
            "s3:prefix" = "processed/*"
          }
        }
      }
    ]
  })
}
