import os
from pyspark.sql import SparkSession, functions as F


def get_spark(app_name="MediTrack360-Bronze-To-Silver"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .getOrCreate()
    )

    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    hconf.set("fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    return spark


def read_csv(spark, path):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def main():
    bucket = os.getenv("S3_BUCKET", "meditrack360-data-lake-6065273c")

    bronze_labs = f"s3a://{bucket}/bronze/lab_results/*/*/*/*.parquet"
    bronze_pharmacy = f"s3a://{bucket}/bronze/pharmacy/*.csv"

    bronze_patients = f"s3a://{bucket}/bronze/postgres/patients/dt=*/patients.csv"
    bronze_admissions = f"s3a://{bucket}/bronze/postgres/admissions/dt=*/admissions.csv"
    bronze_discharges = f"s3a://{bucket}/bronze/postgres/discharges/dt=*/discharges.csv"
    bronze_wards = f"s3a://{bucket}/bronze/postgres/wards/dt=*/wards.csv"

    silver_base = f"s3a://{bucket}/silver"

    spark = get_spark()

    # -------------------------
    # 1) LAB RESULTS (parquet)
    # -------------------------
    labs = spark.read.parquet(bronze_labs)

    # Keep your logic, add safe timestamp normalization so Gold can compute turnaround
    if "sample_time" in labs.columns:
        labs = labs.withColumn("sample_datetime", F.to_timestamp("sample_time"))
    if "completed_time" in labs.columns:
        labs = labs.withColumn("completed_datetime", F.to_timestamp("completed_time"))

    # OPTIONAL: create a stable "collected_time" alias (your Gold uses collected_time)
    # This does NOT remove anything, just adds a compatible column if missing.
    if "collected_time" not in labs.columns:
        if "sample_datetime" in labs.columns:
            labs = labs.withColumn("collected_time", F.col("sample_datetime"))
        elif "sample_time" in labs.columns:
            labs = labs.withColumn("collected_time", F.to_timestamp("sample_time"))
        else:
            labs = labs.withColumn("collected_time", F.current_timestamp())

    # Turnaround hours (your original)
    if "sample_datetime" in labs.columns and "completed_datetime" in labs.columns:
        labs = labs.withColumn(
            "turnaround_hours",
            (
                F.unix_timestamp("completed_datetime")
                - F.unix_timestamp("sample_datetime")
            )
            / 3600.0,
        )

    labs = labs.withColumn("silver_load_timestamp", F.current_timestamp())

    if "sample_datetime" in labs.columns:
        labs = labs.withColumn("test_date", F.to_date("sample_datetime"))

    labs.write.mode("overwrite").parquet(f"{silver_base}/lab_results")
    print("✅ Wrote silver/lab_results")

    # -------------------------
    # 2) PHARMACY (csv)
    # -------------------------
    pharmacy = read_csv(spark, bronze_pharmacy)

    if "stock_on_hand" in pharmacy.columns:
        pharmacy = pharmacy.withColumn(
            "stock_on_hand", F.col("stock_on_hand").cast("int")
        )

    if "reorder_level" in pharmacy.columns:
        pharmacy = pharmacy.withColumn(
            "reorder_level", F.col("reorder_level").cast("int")
        )

    if "expiry_date" in pharmacy.columns:
        pharmacy = pharmacy.withColumn("expiry_date", F.to_date("expiry_date"))

    # OPTIONAL: add aliases used in your current Gold (current_stock, threshold)
    if "current_stock" not in pharmacy.columns and "stock_on_hand" in pharmacy.columns:
        pharmacy = pharmacy.withColumn("current_stock", F.col("stock_on_hand"))
    if "threshold" not in pharmacy.columns and "reorder_level" in pharmacy.columns:
        pharmacy = pharmacy.withColumn("threshold", F.col("reorder_level"))

    pharmacy = pharmacy.withColumn("silver_load_timestamp", F.current_timestamp())

    pharmacy.write.mode("overwrite").parquet(f"{silver_base}/pharmacy_inventory")
    print("✅ Wrote silver/pharmacy_inventory")

    # -------------------------
    # 3) POSTGRES TABLES (csv)
    # -------------------------
    patients = read_csv(spark, bronze_patients)
    admissions = read_csv(spark, bronze_admissions)
    discharges = read_csv(spark, bronze_discharges)
    wards = read_csv(spark, bronze_wards)

    if "dob" in patients.columns:
        patients = patients.withColumn("date_of_birth", F.to_date("dob")).drop("dob")

    patients = patients.withColumn("patient_id", F.col("patient_id").cast("long"))
    patients = patients.withColumn(
        "silver_load_timestamp", F.current_timestamp()
    ).dropDuplicates(["patient_id"])

    for c in ["admission_time", "admitted_at", "admission_datetime"]:
        if c in admissions.columns:
            admissions = admissions.withColumn(c, F.to_timestamp(c))

    for c in ["discharge_time", "discharged_at", "discharge_datetime"]:
        if c in discharges.columns:
            discharges = discharges.withColumn(c, F.to_timestamp(c))

    admissions = admissions.withColumn("patient_id", F.col("patient_id").cast("long"))
    if "admission_id" in admissions.columns:
        admissions = admissions.withColumn(
            "admission_id", F.col("admission_id").cast("long")
        )
    if "ward_id" in admissions.columns:
        admissions = admissions.withColumn("ward_id", F.col("ward_id").cast("long"))

    if "patient_id" in discharges.columns:
        discharges = discharges.withColumn(
            "patient_id", F.col("patient_id").cast("long")
        )
    if "admission_id" in discharges.columns:
        discharges = discharges.withColumn(
            "admission_id", F.col("admission_id").cast("long")
        )

    if "ward_id" in wards.columns:
        wards = wards.withColumn("ward_id", F.col("ward_id").cast("long"))

    patient_adm = admissions
    if "admission_id" in admissions.columns and "admission_id" in discharges.columns:
        patient_adm = patient_adm.join(discharges, on="admission_id", how="left")
    if "ward_id" in patient_adm.columns and "ward_id" in wards.columns:
        patient_adm = patient_adm.join(wards, on="ward_id", how="left")
    if "patient_id" in patient_adm.columns and "patient_id" in patients.columns:
        patient_adm = patient_adm.join(patients, on="patient_id", how="left")

    patient_adm = patient_adm.withColumn("silver_load_timestamp", F.current_timestamp())

    patients.write.mode("overwrite").parquet(f"{silver_base}/patients")
    admissions.write.mode("overwrite").parquet(f"{silver_base}/admissions")
    discharges.write.mode("overwrite").parquet(f"{silver_base}/discharges")
    wards.write.mode("overwrite").parquet(f"{silver_base}/wards")
    patient_adm.write.mode("overwrite").parquet(f"{silver_base}/patient_admissions")

    print("✅ Wrote silver/patients admissions discharges wards patient_admissions")

    spark.stop()
    print("🎉 Bronze → Silver complete")


if __name__ == "__main__":
    main()
