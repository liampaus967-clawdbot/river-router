# ============================================================
# River Router - Terraform Variables
# ============================================================

# ------------------------------------------------------------
# General
# ------------------------------------------------------------

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "river-router"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access resources (your IP)"
  type        = string
  default     = "0.0.0.0/0"  # CHANGE THIS to your IP: "1.2.3.4/32"
}

# ------------------------------------------------------------
# RDS Configuration
# ------------------------------------------------------------

variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"  # 1 GB RAM, free tier eligible
}

variable "rds_storage_gb" {
  description = "Initial storage allocation in GB"
  type        = number
  default     = 50
}

variable "rds_max_storage_gb" {
  description = "Maximum storage for autoscaling in GB"
  type        = number
  default     = 100
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "river_router"
}

variable "db_username" {
  description = "Database master username"
  type        = string
  default     = "river_router"
}

variable "db_password" {
  description = "Database master password"
  type        = string
  sensitive   = true
}

variable "skip_final_snapshot" {
  description = "Skip final snapshot on deletion (set false for production)"
  type        = bool
  default     = true
}

# ------------------------------------------------------------
# EC2 Configuration (Optional)
# ------------------------------------------------------------

variable "create_ec2" {
  description = "Whether to create a new EC2 instance"
  type        = bool
  default     = false  # Set true if you want Terraform to create EC2
}

variable "existing_ec2_sg_id" {
  description = "Security group ID of existing EC2 instance (leave empty if creating new)"
  type        = string
  default     = ""
}

variable "ec2_instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.large"  # 2 vCPU, 8 GB RAM, ~$60/mo
}

variable "ec2_ami" {
  description = "EC2 AMI ID (Ubuntu 22.04 in us-east-1)"
  type        = string
  default     = "ami-0c7217cdde317cfec"  # Ubuntu 22.04 LTS us-east-1
}

variable "ec2_key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
  default     = ""
}

variable "ec2_volume_size" {
  description = "EC2 root volume size in GB"
  type        = number
  default     = 100
}

variable "ssh_port" {
  description = "SSH port number"
  type        = number
  default     = 22
}
