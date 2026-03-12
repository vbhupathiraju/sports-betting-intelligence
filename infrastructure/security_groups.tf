# Security Group for EC2 Producer Instance
resource "aws_security_group" "ec2_producer" {
  name        = "${var.project_name}-ec2-producer-sg"
  description = "Security group for EC2 Kafka producer instances"
  vpc_id      = aws_vpc.main.id

  # Allow outbound traffic to MSK on Kafka TLS port
  egress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka TLS to MSK"
  }

  # Allow outbound HTTPS for API calls (Odds API, Kalshi, Secrets Manager)
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS outbound for external APIs"
  }

  # Allow outbound HTTP (needed for some health checks)
  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP outbound"
  }

  # Allow SSH from your IP only — we'll lock this down further
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH access - restrict to your IP after setup"
  }

  tags = {
    Name    = "${var.project_name}-ec2-producer-sg"
    Project = var.project_name
  }
}

# Security Group for MSK Cluster
resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "Security group for MSK Kafka cluster"
  vpc_id      = aws_vpc.main.id

  # Only accept Kafka TLS connections from EC2 producer security group
  ingress {
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_producer.id]
    description     = "Kafka TLS from EC2 producers"
  }

  # Accept Kafka IAM auth port from EC2 producers
  ingress {
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_producer.id]
    description     = "Kafka IAM auth from EC2 producers"
  }

  # Allow all outbound within VPC
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
    description = "All outbound within VPC"
  }

  tags = {
    Name    = "${var.project_name}-msk-sg"
    Project = var.project_name
  }
}
