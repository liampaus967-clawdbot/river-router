# ============================================================
# River Router - Terraform Outputs
# ============================================================

# ------------------------------------------------------------
# RDS Outputs
# ------------------------------------------------------------

output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.main.endpoint
}

output "rds_hostname" {
  description = "RDS instance hostname (without port)"
  value       = aws_db_instance.main.address
}

output "rds_port" {
  description = "RDS instance port"
  value       = aws_db_instance.main.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.main.db_name
}

output "rds_username" {
  description = "RDS master username"
  value       = aws_db_instance.main.username
}

output "database_url" {
  description = "Full database connection URL (password redacted)"
  value       = "postgresql://${aws_db_instance.main.username}:****@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}"
}

output "database_url_ssm_param" {
  description = "SSM parameter name containing full connection string"
  value       = aws_ssm_parameter.db_connection_string.name
}

# ------------------------------------------------------------
# Security Group Outputs
# ------------------------------------------------------------

output "rds_security_group_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds.id
}

output "ec2_security_group_id" {
  description = "EC2 security group ID (if created)"
  value       = var.create_ec2 ? aws_security_group.ec2[0].id : null
}

# ------------------------------------------------------------
# EC2 Outputs (if created)
# ------------------------------------------------------------

output "ec2_public_ip" {
  description = "EC2 public IP address (if created)"
  value       = var.create_ec2 ? aws_instance.api[0].public_ip : null
}

output "ec2_instance_id" {
  description = "EC2 instance ID (if created)"
  value       = var.create_ec2 ? aws_instance.api[0].id : null
}

# ------------------------------------------------------------
# Connection Instructions
# ------------------------------------------------------------

output "connection_instructions" {
  description = "How to connect to the database"
  value       = <<-EOT
    
    ============================================================
    River Router - Database Connection
    ============================================================
    
    1. Connect to RDS and enable PostGIS:
       
       psql -h ${aws_db_instance.main.address} -U ${aws_db_instance.main.username} -d ${aws_db_instance.main.db_name}
       
       CREATE EXTENSION postgis;
       CREATE EXTENSION postgis_topology;
    
    2. Set environment variable on your EC2:
       
       export DATABASE_URL="postgresql://${aws_db_instance.main.username}:<password>@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}"
    
    3. Or retrieve from SSM Parameter Store:
       
       aws ssm get-parameter --name "/${var.project_name}/db/connection_string" --with-decryption --query 'Parameter.Value' --output text
    
    ============================================================
  EOT
}
