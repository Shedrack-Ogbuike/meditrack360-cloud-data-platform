# MediTrack360 Setup Guide

## Prerequisites

### Software Requirements
- **Docker** 20.10+ and **Docker Compose** 1.29+
- **Python** 3.9+ (for local development)
- **Terraform** 1.0+ (for AWS deployment)
- **AWS CLI** configured with credentials
- **Git** for version control

### AWS Requirements
- AWS Account with appropriate permissions
- IAM User with:
  - AmazonS3FullAccess
  - AmazonRedshiftFullAccess  
  - AWSCloudFormationFullAccess
  - IAMFullAccess (for Terraform)

### System Requirements
- **CPU**: 4+ cores recommended
- **Memory**: 8GB+ RAM
- **Storage**: 20GB+ free disk space
- **OS**: Linux, macOS, or Windows (WSL2)

---

## Quick Start (Local Development)

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/meditrack360.git
cd meditrack360