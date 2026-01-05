terraform {
  required_version = ">= 1.0"
}

provider "aws" {
  region = "us-east-2"
}

resource "aws_s3_bucket" "terraform_state" {
  bucket = "meditrack360-terraform-state-368740523607"
  
  tags = {
    Name    = "Terraform State Bucket"
    Project = "MediTrack360"
  }
}
