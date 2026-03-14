# IAM Role for EC2 Producer
resource "aws_iam_role" "ec2_producer" {
  name = "${var.project_name}-ec2-producer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

# IAM Policy for EC2 — scoped to exactly what producers need
resource "aws_iam_role_policy" "ec2_producer" {
  name = "${var.project_name}-ec2-producer-policy"
  role = aws_iam_role.ec2_producer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MSKAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:DescribeClusterDynamicConfiguration",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup",
          "kafka:GetBootstrapBrokers",
          "kafka:DescribeCluster",
          "kafka:ListClusters"
        ]
        Resource = "*"
      },
      {
        Sid    = "SecretsManagerAccess"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:sports-betting/*"
      },
      {
        Sid    = "CloudWatchAccess"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Sid    = "FirehoseAccess"
        Effect = "Allow"
        Action = [
          "firehose:PutRecord",
          "firehose:PutRecordBatch"
        ]
        Resource = [
          "arn:aws:firehose:${var.aws_region}:${var.account_id}:deliverystream/sports-betting-odds-stream",
          "arn:aws:firehose:${var.aws_region}:${var.account_id}:deliverystream/sports-betting-kalshi-stream",
          "arn:aws:firehose:${var.aws_region}:${var.account_id}:deliverystream/sports-betting-game-events-stream"
        ]
      }
    ]
  })
}

# Instance Profile — attaches IAM role to EC2
resource "aws_iam_instance_profile" "ec2_producer" {
  name = "${var.project_name}-ec2-producer-profile"
  role = aws_iam_role.ec2_producer.name
}

# Find latest Amazon Linux 2023 AMI for ARM (M4 compatible)
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# EC2 Producer Instance
resource "aws_instance" "producer" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = "t4g.small"
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.ec2_producer.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_producer.name

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    yum install -y docker python3 python3-pip
    systemctl start docker
    systemctl enable docker
    usermod -a -G docker ec2-user
    pip3 install boto3 kafka-python requests websockets
  EOF

  tags = {
    Name    = "${var.project_name}-producer"
    Project = var.project_name
  }
}

# Add EC2 instance ID to outputs
