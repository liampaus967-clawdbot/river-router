# River Router - Terraform Infrastructure

This Terraform configuration provisions the AWS infrastructure for River Router:

- **RDS PostgreSQL 16** with PostGIS support
- **Security groups** for RDS and EC2 communication
- **SSM Parameter Store** for secure credential storage
- **Optional: EC2 instance** for the API server

## Prerequisites

1. [Terraform](https://www.terraform.io/downloads) >= 1.0
2. [AWS CLI](https://aws.amazon.com/cli/) configured with credentials
3. An AWS account with permissions to create RDS, EC2, VPC resources

## Quick Start

```bash
# Navigate to terraform directory
cd terraform

# Copy example variables
cp terraform.tfvars.example terraform.tfvars

# Edit with your values (especially db_password and allowed_cidr)
nano terraform.tfvars  # or your preferred editor

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply (creates resources)
terraform apply
```

## Configuration

Edit `terraform.tfvars` with your values:

| Variable | Description | Example |
|----------|-------------|---------|
| `db_password` | Database password | `MyStr0ngP@ssword!` |
| `allowed_cidr` | Your IP for access | `1.2.3.4/32` |
| `aws_region` | AWS region | `us-east-1` |
| `rds_instance_class` | RDS size | `db.t3.small` |
| `existing_ec2_sg_id` | Your EC2's security group | `sg-0123456789abcdef0` |

### Finding Your IP

```bash
curl ifconfig.me
# Then use: allowed_cidr = "YOUR_IP/32"
```

### Finding Your EC2 Security Group

AWS Console → EC2 → Instances → Select your instance → Security tab → Security groups

## After Apply

### 1. Enable PostGIS

Connect to the database and enable the PostGIS extension:

```bash
# Get connection details from Terraform output
terraform output rds_endpoint

# Connect with psql
psql -h <rds_endpoint> -U river_router -d river_router

# Enable PostGIS
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;

# Verify
SELECT PostGIS_Version();
```

### 2. Get Connection String

```bash
# From Terraform output (password redacted)
terraform output database_url

# From SSM Parameter Store (full connection string with password)
aws ssm get-parameter \
  --name "/river-router/db/connection_string" \
  --with-decryption \
  --query 'Parameter.Value' \
  --output text
```

### 3. Set Environment Variable on EC2

```bash
# On your EC2 instance
export DATABASE_URL=$(aws ssm get-parameter \
  --name "/river-router/db/connection_string" \
  --with-decryption \
  --query 'Parameter.Value' \
  --output text)
```

## Estimated Costs

| Resource | Size | Monthly Cost |
|----------|------|--------------|
| RDS `db.t3.micro` | 1 GB RAM | ~$15 |
| RDS `db.t3.small` | 2 GB RAM | ~$30 |
| RDS `db.t3.medium` | 4 GB RAM | ~$60 |
| RDS Storage | 50 GB gp3 | ~$6 |
| EC2 `t3.large` | 8 GB RAM | ~$60 |

## Destroying Resources

```bash
# Destroy all resources (WARNING: deletes database!)
terraform destroy
```

## Security Notes

- `terraform.tfvars` contains secrets — **never commit to git**
- RDS is publicly accessible but protected by security group
- Use `allowed_cidr` to restrict access to your IP only
- Credentials are stored in SSM Parameter Store (encrypted)
- For production: set `skip_final_snapshot = false` and `deletion_protection = true`

## Troubleshooting

### Can't connect to RDS

1. Check security group allows your IP
2. Check RDS is publicly accessible
3. Verify password is correct

```bash
# Test connectivity
nc -zv <rds_endpoint> 5432
```

### PostGIS extension fails

PostgreSQL 16 includes PostGIS in RDS by default. If `CREATE EXTENSION` fails:

```sql
-- Check available extensions
SELECT * FROM pg_available_extensions WHERE name LIKE 'postgis%';
```
