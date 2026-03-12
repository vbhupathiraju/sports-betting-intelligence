# MSK Configuration
resource "aws_msk_configuration" "main" {
  name              = "${var.project_name}-msk-config"
  kafka_versions    = ["3.6.0"]
  server_properties = <<PROPERTIES
auto.create.topics.enable=false
default.replication.factor=2
min.insync.replicas=1
num.io.threads=8
num.network.threads=5
num.partitions=3
num.replica.fetchers=2
replica.lag.time.max.ms=30000
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
unclean.leader.election.enable=true
zookeeper.session.timeout.ms=18000
PROPERTIES
}

# MSK Cluster
resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-msk-cluster"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type  = "kafka.t3.small"
    client_subnets = [
      aws_subnet.private[0].id,
      aws_subnet.private[1].id
    ]
    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  # IAM authentication — no username/password
  client_authentication {
    sasl {
      iam = true
    }
  }

  # Encryption in transit — TLS only
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  # Enhanced monitoring for CloudWatch
  enhanced_monitoring = "DEFAULT"

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = false
      }
      node_exporter {
        enabled_in_broker = false
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = "/aws/msk/${var.project_name}"
      }
    }
  }

  tags = {
    Name    = "${var.project_name}-msk-cluster"
    Project = var.project_name
  }
}

# CloudWatch Log Group for MSK
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project_name}"
  retention_in_days = 7

  tags = {
    Project = var.project_name
  }
}
