# ============================================================
# River Router - AWS Infrastructure
# ============================================================
# This Terraform configuration provisions:
# - RDS PostgreSQL with PostGIS
# - Security groups for RDS and EC2 communication
# - Optional: EC2 instance for the API server
#
# Usage:
#   cd terraform
#   cp terraform.tfvars.example terraform.tfvars
#   # Edit terraform.tfvars with your values
#   terraform init
#   terraform plan
#   terraform apply
# ============================================================

terraform {
  required_version = ">= 1.0"
  
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

# ============================================================
# Data Sources
# ============================================================

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# Get available AZs
data "aws_availability_zones" "available" {
  state = "available"
}

# Get default VPC (or specify your own)
data "aws_vpc" "default" {
  default = true
}

# Get default subnets
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ============================================================
# Security Groups
# ============================================================

# Security group for RDS
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group for River Router RDS"
  vpc_id      = data.aws_vpc.default.id

  # PostgreSQL from EC2 security group
  ingress {
    description     = "PostgreSQL from EC2"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = var.existing_ec2_sg_id != "" ? [var.existing_ec2_sg_id] : []
    cidr_blocks     = var.existing_ec2_sg_id == "" ? [var.allowed_cidr] : []
  }

  # PostgreSQL from your IP (for direct access during development)
  ingress {
    description = "PostgreSQL from allowed IP"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-rds-sg"
    Project = var.project_name
  }
}

# Security group for EC2 API server (if creating new)
resource "aws_security_group" "ec2" {
  count = var.create_ec2 ? 1 : 0

  name        = "${var.project_name}-ec2-sg"
  description = "Security group for River Router API server"
  vpc_id      = data.aws_vpc.default.id

  # SSH
  ingress {
    description = "SSH"
    from_port   = var.ssh_port
    to_port     = var.ssh_port
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  # HTTP API
  ingress {
    description = "HTTP API"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTPS (if using nginx/certbot)
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-ec2-sg"
    Project = var.project_name
  }
}

# ============================================================
# RDS PostgreSQL
# ============================================================

# DB subnet group
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Name    = "${var.project_name}-db-subnet"
    Project = var.project_name
  }
}

# RDS Parameter Group for PostGIS
resource "aws_db_parameter_group" "postgres" {
  name   = "${var.project_name}-postgres16"
  family = "postgres16"

  # PostGIS requires shared_preload_libraries
  parameter {
    name  = "shared_preload_libraries"
    value = "pg_stat_statements"
  }

  tags = {
    Name    = "${var.project_name}-param-group"
    Project = var.project_name
  }
}

# RDS Instance
resource "aws_db_instance" "main" {
  identifier = "${var.project_name}-db"

  # Engine
  engine               = "postgres"
  engine_version       = "16.4"
  instance_class       = var.rds_instance_class
  parameter_group_name = aws_db_parameter_group.postgres.name

  # Storage
  allocated_storage     = var.rds_storage_gb
  max_allocated_storage = var.rds_max_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true

  # Credentials
  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  # Network
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true  # Required for access from EC2 in different subnet
  port                   = 5432

  # Backup & Maintenance
  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"

  # Performance Insights (free tier)
  performance_insights_enabled = true
  performance_insights_retention_period = 7

  # Other
  multi_az               = false  # Set true for production
  skip_final_snapshot    = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : "${var.project_name}-final-snapshot"
  deletion_protection    = false  # Set true for production

  tags = {
    Name    = "${var.project_name}-db"
    Project = var.project_name
  }
}

# ============================================================
# EC2 Instance (Optional)
# ============================================================

resource "aws_instance" "api" {
  count = var.create_ec2 ? 1 : 0

  ami           = var.ec2_ami
  instance_type = var.ec2_instance_type

  vpc_security_group_ids = [aws_security_group.ec2[0].id]
  key_name               = var.ec2_key_name

  root_block_device {
    volume_size = var.ec2_volume_size
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name    = "${var.project_name}-api"
    Project = var.project_name
  }
}

# ============================================================
# SSM Parameter Store (secure credential storage)
# ============================================================

resource "aws_ssm_parameter" "db_password" {
  name        = "/${var.project_name}/db/password"
  description = "River Router database password"
  type        = "SecureString"
  value       = var.db_password

  tags = {
    Project = var.project_name
  }
}

resource "aws_ssm_parameter" "db_connection_string" {
  name        = "/${var.project_name}/db/connection_string"
  description = "River Router database connection string"
  type        = "SecureString"
  value       = "postgresql://${var.db_username}:${var.db_password}@${aws_db_instance.main.endpoint}/${var.db_name}"

  tags = {
    Project = var.project_name
  }
}
