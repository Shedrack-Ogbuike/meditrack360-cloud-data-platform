# spark/utils/config.py
"""
Configuration for MediTrack360 Spark transformations
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
    
    # S3 Configuration
    S3_BUCKET = os.getenv("S3_BUCKET", "meditrack360-data-lake-6065273c")
    BRONZE_PREFIX = "bronze"
    SILVER_PREFIX = "silver"
    GOLD_PREFIX = "gold"
    
    # Database Configuration (from your extraction)
    POSTGRES_CONFIG = {
        "host": os.getenv("PG_HOST", "aws-1-eu-central-1.pooler.supabase.com"),
        "port": os.getenv("PG_PORT", 5432),
        "database": os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "medi_reader.hceprxhtdgtbqmrfwymn"),
        "password": os.getenv("PG_PWD", "medi_reader123")
    }
    
    # API Configuration (from your extraction)
    PHARMACY_API_URL = os.getenv("PHARMACY_API_URL", 
        "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json")
    
    # Spark Configuration
    SPARK_CONFIG = {
        "app_name": "MediTrack360-Data-Pipeline",
        "master": "local[*]",
        "executor_memory": "2g",
        "driver_memory": "2g",
        "spark.sql.shuffle.partitions": "200",
        "spark.default.parallelism": "200"
    }
    
    # Data Quality Configuration
    VALIDATION_CONFIG = {
        "ge_context_root_dir": "./great_expectations",
        "expectation_suite_prefix": "meditrack360",
        "validation_store_prefix": "s3://{bucket}/silver/validation_store"
    }
    
    @classmethod
    def get_s3_path(cls, layer, data_type):
        """Get S3 path for different data layers"""
        return f"s3a://{cls.S3_BUCKET}/{layer}/{data_type}"
    
    @classmethod
    def get_bronze_path(cls, data_type):
        return cls.get_s3_path("bronze", data_type)
    
    @classmethod
    def get_silver_path(cls, data_type):
        return cls.get_s3_path("silver", data_type)
    
    @classmethod
    def get_gold_path(cls, data_type):
        return cls.get_s3_path("gold", data_type)