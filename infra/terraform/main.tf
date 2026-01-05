terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "meditrack360-terraform-state-368740523607"
    key    = "terraform.tfstate"
    region = "us-east-2"
  }
}

provider "aws" {
  region = "us-east-2"
}

# S3 Buckets (using existing hex: 6065273c)
resource "aws_s3_bucket" "data_lake" {
  bucket = "meditrack360-data-lake-6065273c"
  
  tags = {
    Name        = "MediTrack360 Data Lake"
    Environment = "dev"
    Project     = "MediTrack360"
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "meditrack360-athena-results-6065273c"
  
  tags = {
    Name        = "MediTrack360 Athena Results"
    Environment = "dev"
    Project     = "MediTrack360"
  }
}

# IAM Role for Athena Service
resource "aws_iam_role" "athena_role" {
  name = "MediTrack360-Athena-Role-dev"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "athena.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = "dev"
    Project     = "MediTrack360"
  }
}

# Attach managed policies to the role
resource "aws_iam_role_policy_attachment" "athena_full" {
  role       = aws_iam_role.athena_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "s3_full" {
  role       = aws_iam_role.athena_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Athena Resources
resource "aws_athena_workgroup" "meditrack360" {
  name = "meditrack360-dev-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://meditrack360-athena-results-6065273c/queries/"
    }
  }
}

resource "aws_athena_database" "meditrack360" {
  name   = "meditrack360_dev"
  bucket = aws_s3_bucket.athena_results.bucket
}

# Outputs
output "s3_data_lake" {
  value = aws_s3_bucket.data_lake.bucket
}

output "s3_athena_results" {
  value = aws_s3_bucket.athena_results.bucket
}

output "athena_workgroup" {
  value = aws_athena_workgroup.meditrack360.name
}

output "athena_database" {
  value = aws_athena_database.meditrack360.name
}

output "athena_role_arn" {
  value = aws_iam_role.athena_role.arn
}
