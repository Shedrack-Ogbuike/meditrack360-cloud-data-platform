variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {
    Project     = "MediTrack360"
    ManagedBy   = "Terraform"
    Department  = "Healthcare"
  }
}
