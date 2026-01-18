"""
Simple Silver to Gold transformation
Creates analytics-ready data in S3
Athena setup will be done separately in AWS Console
"""

import os
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit

logger = logging.getLogger(__name__)


def get_spark(app_name="MediTrack360-Gold"):
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    return spark


def get_silver_path(table):
    bucket = "meditrack360-data-lake-6065273c"
    return f"s3a://{bucket}/silver/{table}"


def get_gold_path(table):
    bucket = "meditrack360-data-lake-6065273c"
    return f"s3a://{bucket}/gold/{table}"


def transform_silver_to_gold():
    logger.info("Starting simple Silver to Gold transformation")
    spark = get_spark(app_name="MediTrack360-Gold-Simple")

    try:
        create_simple_dimensions(spark)
        create_simple_facts(spark)
        create_summary_tables(spark)
        logger.info(" Gold transformation completed successfully")
    except Exception as e:
        logger.error(f" Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


def create_simple_dimensions(spark):
    # 1. dim_patient 
    try:
        patients = spark.read.parquet(get_silver_path("patients"))
        dim_patient = patients.select(
            F.col("patient_id").alias("patient_key"),
            "patient_id",
            "first_name",
            "last_name",
            F.col("date_of_birth").alias("birth_date"),
            F.when(
                F.col("date_of_birth").isNotNull(),
                F.year(F.current_date()) - F.year(F.col("date_of_birth")),
            ).alias("age"),
            "gender",
            F.current_timestamp().alias("created_at"),
        ).distinct()

        dim_patient.write.mode("overwrite").parquet(get_gold_path("dim_patient"))
        logger.info(f"Created dim_patient: {dim_patient.count()} rows")
    except Exception as e:
        logger.error(f"Failed dim_patient: {str(e)}")

    # 2. dim_ward 
    try:
        wards = spark.read.parquet(get_silver_path("wards"))
        dim_ward = wards.select(
            F.col("ward_id").alias("ward_key"),
            "ward_id",
            "ward_name",
            F.current_timestamp().alias("created_at"),
        ).distinct()

        dim_ward.write.mode("overwrite").parquet(get_gold_path("dim_ward"))
        logger.info(f"Created dim_ward: {dim_ward.count()} rows")
    except Exception as e:
        logger.error(f"Failed dim_ward: {str(e)}")

    # 3. dim_date 
    try:
        dates = spark.sql(
            "SELECT explode(sequence(to_date('2019-01-01'), to_date('2026-12-31'), interval 1 day)) AS d"
        )
        dim_date = dates.select(
            F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("d").alias("full_date"),
            F.year("d").alias("year"),
            F.month("d").alias("month"),
            F.dayofmonth("d").alias("day"),
            F.date_format("d", "EEEE").alias("day_name"),
            F.date_format("d", "MMMM").alias("month_name"),
            F.current_timestamp().alias("created_at"),
        )

        dim_date.write.mode("overwrite").parquet(get_gold_path("dim_date"))
        logger.info(f"Created dim_date: {dim_date.count()} rows")
    except Exception as e:
        logger.error(f"Failed dim_date: {str(e)}")

    # 4. dim_drug 
    try:
        pharmacy = spark.read.parquet(get_silver_path("pharmacy_inventory"))
        if "drug_name" in pharmacy.columns:
            dim_drug = pharmacy.select(
                F.monotonically_increasing_id().alias("drug_key"),
                F.col("drug_name"),
                F.current_timestamp().alias("created_at"),
            ).dropDuplicates(["drug_name"])

            dim_drug.write.mode("overwrite").parquet(get_gold_path("dim_drug"))
            logger.info(f"Created dim_drug: {dim_drug.count()} rows")
        else:
            logger.warning(
                "dim_drug skipped: 'drug_name' missing in silver/pharmacy_inventory"
            )
    except Exception as e:
        logger.error(f"Failed dim_drug: {str(e)}")


def create_simple_facts(spark):
    # 1. fact_admissions 
    try:
        admissions = spark.read.parquet(get_silver_path("admissions"))
        fact_admissions = admissions.select(
            F.monotonically_increasing_id().alias("admission_key"),
            "admission_id",
            "patient_id",
            "ward_id",
            F.coalesce(F.col("admission_time"), F.current_timestamp()).alias(
                "admission_time"
            ),
            F.current_timestamp().alias("created_at"),
        ).distinct()

        fact_admissions.write.mode("overwrite").parquet(
            get_gold_path("fact_admissions")
        )
        logger.info(f"Created fact_admissions: {fact_admissions.count()} rows")
    except Exception as e:
        logger.error(f"Failed fact_admissions: {str(e)}")

    # 2. fact_lab_turnaround 
    try:
        labs = spark.read.parquet(get_silver_path("lab_results"))
        fact_labs = labs.select(
            F.monotonically_increasing_id().alias("lab_key"),
            "patient_id",
            F.coalesce(F.col("collected_time"), F.current_timestamp()).alias(
                "sample_time"
            ),
            F.current_timestamp().alias("created_at"),
        ).distinct()
        fact_labs.write.mode("overwrite").parquet(get_gold_path("fact_labs"))
        logger.info(f"Created fact_labs: {fact_labs.count()} rows")

        sample_ts = F.coalesce(
            F.col("sample_datetime"),
            (
                F.to_timestamp(F.col("sample_time"))
                if "sample_time" in labs.columns
                else lit(None)
            ),
            (
                F.to_timestamp(F.col("collected_time"))
                if "collected_time" in labs.columns
                else lit(None)
            ),
            F.current_timestamp(),
        )
        completed_ts = F.coalesce(
            F.col("completed_datetime"),
            (
                F.to_timestamp(F.col("completed_time"))
                if "completed_time" in labs.columns
                else lit(None)
            ),
            F.current_timestamp(),
        )

        fact_lab_turnaround = labs.select(
            F.monotonically_increasing_id().alias("lab_turnaround_key"),
            F.col("patient_id"),
            sample_ts.alias("sample_time"),
            completed_ts.alias("completed_time"),
            (
                (F.unix_timestamp(completed_ts) - F.unix_timestamp(sample_ts)) / 60.0
            ).alias("turnaround_minutes"),
            F.current_timestamp().alias("created_at"),
        ).distinct()

        fact_lab_turnaround.write.mode("overwrite").parquet(
            get_gold_path("fact_lab_turnaround")
        )
        logger.info(f"Created fact_lab_turnaround: {fact_lab_turnaround.count()} rows")

    except Exception as e:
        logger.error(f"Failed labs facts: {str(e)}")

    # 3. fact_pharmacy_stock 
    try:
        pharmacy = spark.read.parquet(get_silver_path("pharmacy_inventory"))

        fact_pharmacy = pharmacy.select(
            F.monotonically_increasing_id().alias("inventory_key"),
            "drug_name",
            F.coalesce(F.col("current_stock"), lit(0))
            .cast("int")
            .alias("stock_on_hand"),
            F.coalesce(F.col("threshold"), lit(10)).cast("int").alias("reorder_level"),
            F.current_timestamp().alias("created_at"),
        ).distinct()
        fact_pharmacy.write.mode("overwrite").parquet(get_gold_path("fact_pharmacy"))
        logger.info(f"Created fact_pharmacy: {fact_pharmacy.count()} rows")

        fact_pharmacy.write.mode("overwrite").parquet(
            get_gold_path("fact_pharmacy_stock")
        )
        logger.info(f"Created fact_pharmacy_stock: {fact_pharmacy.count()} rows")

    except Exception as e:
        logger.error(f"Failed fact_pharmacy / fact_pharmacy_stock: {str(e)}")

    # 4. fact_icu_alerts 
    try:
        schema = "alert_id long, patient_id long, alert_time timestamp, alert_type string, severity string, created_at timestamp"
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df.write.mode("overwrite").parquet(get_gold_path("fact_icu_alerts"))
        logger.info("Created fact_icu_alerts (empty schema-ready)")
    except Exception as e:
        logger.error(f"Failed fact_icu_alerts: {str(e)}")


def create_summary_tables(spark):
    # 1. Patient admission summary 
    try:
        try:
            fact_admissions = spark.read.parquet(get_gold_path("fact_admissions"))
            patient_admission_counts = fact_admissions.groupBy("patient_id").agg(
                F.count("*").alias("admission_count")
            )
        except BaseException:
            logger.warning("fact_admissions not found, using dummy admission counts")
            dim_patient = spark.read.parquet(get_gold_path("dim_patient"))
            patient_admission_counts = dim_patient.select(
                "patient_id", F.lit(1).alias("admission_count")
            )

        dim_patient = spark.read.parquet(get_gold_path("dim_patient"))
        patient_summary = dim_patient.join(
            patient_admission_counts, "patient_id", "left"
        ).select(
            "patient_key",
            "patient_id",
            "first_name",
            "last_name",
            "age",
            "gender",
            F.coalesce("admission_count", F.lit(0)).alias("total_admissions"),
            F.current_timestamp().alias("created_at"),
        )

        patient_summary.write.mode("overwrite").parquet(
            get_gold_path("patient_summary")
        )
        logger.info(f"Created patient_summary: {patient_summary.count()} rows")
    except Exception as e:
        logger.error(f"Failed patient_summary: {str(e)}")

    # 2. Daily admissions summary 
    try:
        fact_admissions = spark.read.parquet(get_gold_path("fact_admissions"))
        daily_summary = (
            fact_admissions.groupBy(
                F.date_format("admission_time", "yyyy-MM-dd").alias("admission_date")
            )
            .agg(
                F.count("*").alias("daily_admissions"),
                F.countDistinct("patient_id").alias("unique_patients"),
                F.countDistinct("ward_id").alias("active_wards"),
            )
            .orderBy("admission_date")
        )

        daily_summary.write.mode("overwrite").parquet(
            get_gold_path("daily_admissions_summary")
        )
        logger.info(f"Created daily_admissions_summary: {daily_summary.count()} rows")
    except Exception as e:
        logger.error(f"Failed daily_admissions_summary: {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    transform_silver_to_gold()
