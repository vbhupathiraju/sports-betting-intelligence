output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

output "msk_bootstrap_brokers_iam" {
  description = "MSK bootstrap brokers for IAM auth"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "ec2_producer_sg_id" {
  description = "EC2 producer security group ID"
  value       = aws_security_group.ec2_producer.id
}

output "msk_sg_id" {
  description = "MSK security group ID"
  value       = aws_security_group.msk.id
}

output "ec2_producer_instance_id" {
  description = "EC2 producer instance ID"
  value       = aws_instance.producer.id
}

output "ec2_producer_private_ip" {
  description = "EC2 producer private IP"
  value       = aws_instance.producer.private_ip
}
