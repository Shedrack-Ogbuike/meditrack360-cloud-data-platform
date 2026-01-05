"""
Silver to Gold transformation with validation
"""
import os
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def validate_gold_table(df, table_name):
    """Validate gold table data quality"""
    validation_results = {
        'table': table_name,
        'validation_time': datetime.utcnow().isoformat(),
        'row_count': df.count(),
        'issues': [],
        'passed': True
    }
    
    try:
        # Dimension table validations
        if table_name.startswith('dim_'):
            # Check for null dimension keys
            if 'patient_key' in df.columns or 'ward_key' in df.columns:
                key_col = 'patient_key' if 'patient_key' in df.columns else 'ward_key'
                null_count = df.filter(col(key_col).isNull()).count()
                if null_count > 0:
                    validation_results['issues'].append(f'{null_count} null values in dimension key')
                    validation_results['passed'] = False
            
            # Check for duplicate dimension keys
            if 'patient_key' in df.columns:
                duplicate_count = df.groupBy('patient_key').count().filter(col('count') > 1).count()
                if duplicate_count > 0:
                    validation_results['issues'].append(f'{duplicate_count} duplicate patient keys')
                    validation_results['passed'] = False
            
            # Check for surrogate key uniqueness
            if table_name == 'dim_patient' and 'patient_key' in df.columns:
                total_count = df.count()
                unique_keys = df.select('patient_key').distinct().count()
                if total_count != unique_keys:
                    validation_results['issues'].append(f'Dimension keys not unique: {total_count} rows, {unique_keys} unique keys')
                    validation_results['passed'] = False
        
        # Fact table validations
        elif table_name.startswith('fact_'):
            # Check for null foreign keys
            if 'patient_id' in df.columns:
                null_patients = df.filter(col('patient_id').isNull()).count()
                if null_patients > 0:
                    validation_results['issues'].append(f'{null_patients} null patient_ids in fact table')
                    validation_results['passed'] = False
            
            if table_name == 'fact_admissions' and 'admission_id' in df.columns:
                null_admissions = df.filter(col('admission_id').isNull()).count()
                if null_admissions > 0:
                    validation_results['issues'].append(f'{null_admissions} null admission_ids')
                    validation_results['passed'] = False
            
            if table_name == 'fact_pharmacy':
                if 'stock_on_hand' in df.columns:
                    negative_stock = df.filter(col('stock_on_hand') < 0).count()
                    if negative_stock > 0:
                        validation_results['issues'].append(f'{negative_stock} negative stock values')
                        validation_results['passed'] = False
                
                if 'reorder_level' in df.columns and 'stock_on_hand' in df.columns:
                    # Check if stock is below reorder level
                    needs_reorder = df.filter(col('stock_on_hand') < col('reorder_level')).count()
                    validation_results['needs_reorder'] = int(needs_reorder)
        
        # Summary table validations
        elif table_name == 'patient_summary':
            if 'total_admissions' in df.columns:
                negative_admissions = df.filter(col('total_admissions') < 0).count()
                if negative_admissions > 0:
                    validation_results['issues'].append(f'{negative_admissions} negative admission counts')
                    validation_results['passed'] = False
            
            if 'age' in df.columns:
                invalid_age = df.filter((col('age') < 0) | (col('age') > 120)).count()
                if invalid_age > 0:
                    validation_results['issues'].append(f'{invalid_age} invalid age values')
                    validation_results['passed'] = False
        
        elif table_name == 'daily_admissions_summary':
            if 'daily_admissions' in df.columns:
                negative_daily = df.filter(col('daily_admissions') < 0).count()
                if negative_daily > 0:
                    validation_results['issues'].append(f'{negative_daily} negative daily admissions')
                    validation_results['passed'] = False
            
            if 'unique_patients' in df.columns and 'daily_admissions' in df.columns:
                # Check if unique patients > daily admissions (shouldn't happen)
                illogical = df.filter(col('unique_patients') > col('daily_admissions')).count()
                if illogical > 0:
                    validation_results['issues'].append(f'{illogical} rows where unique_patients > daily_admissions')
                    validation_results['passed'] = False
    
    except Exception as e:
        validation_results['issues'].append(f'Validation error: {str(e)}')
        validation_results['passed'] = False
    
    return validation_results

def save_gold_validation_results(spark, validation_results, table_name, execution_date):
    """Save gold validation results to S3"""
    bucket = os.getenv("S3_BUCKET", "meditrack360-data-lake-6065273c")
    
    # Convert to DataFrame and save
    validation_df = spark.createDataFrame([validation_results])
    
    validation_path = f"s3a://{bucket}/gold/validation/{execution_date}/{table_name}_validation"
    validation_df.write.mode('append').json(validation_path)
    
    return validation_path

def check_referential_integrity(spark, bucket, execution_date):
    """Check referential integrity between fact and dimension tables"""
    integrity_results = {
        'check_time': datetime.utcnow().isoformat(),
        'execution_date': execution_date,
        'checks': {},
        'overall_status': 'PASS'
    }
    
    try:
        # Read dimension tables
        dim_patient = spark.read.parquet(f"s3a://{bucket}/gold/dim_patient")
        dim_ward = spark.read.parquet(f"s3a://{bucket}/gold/dim_ward")
        
        # Get all valid IDs
        valid_patient_ids = {row['patient_id'] for row in dim_patient.select('patient_id').distinct().collect()}
        valid_ward_ids = {row['ward_id'] for row in dim_ward.select('ward_id').distinct().collect()}
        
        # Check fact_admissions
        fact_admissions = spark.read.parquet(f"s3a://{bucket}/gold/fact_admissions")
        
        # Patients in fact_admissions not in dim_patient
        if 'patient_id' in fact_admissions.columns:
            missing_patients = fact_admissions.filter(~col('patient_id').isin(list(valid_patient_ids))).count()
            integrity_results['checks']['fact_admissions_patient_integrity'] = {
                'missing_patients': int(missing_patients),
                'status': 'PASS' if missing_patients == 0 else 'FAIL'
            }
            
            if missing_patients > 0:
                integrity_results['overall_status'] = 'FAIL'
                logger.warning(f"Referential integrity: {missing_patients} patient_ids in fact_admissions not found in dim_patient")
        
        # Wards in fact_admissions not in dim_ward
        if 'ward_id' in fact_admissions.columns:
            missing_wards = fact_admissions.filter(~col('ward_id').isin(list(valid_ward_ids))).count()
            integrity_results['checks']['fact_admissions_ward_integrity'] = {
                'missing_wards': int(missing_wards),
                'status': 'PASS' if missing_wards == 0 else 'FAIL'
            }
            
            if missing_wards > 0:
                integrity_results['overall_status'] = 'FAIL'
                logger.warning(f"Referential integrity: {missing_wards} ward_ids in fact_admissions not found in dim_ward")
    
    except Exception as e:
        integrity_results['checks']['error'] = str(e)
        integrity_results['overall_status'] = 'FAIL'
        logger.error(f"Failed to check referential integrity: {str(e)}")
    
    # Save integrity results
    integrity_path = f"s3a://{bucket}/gold/validation/{execution_date}/referential_integrity"
    spark.createDataFrame([integrity_results]).write.mode('append').json(integrity_path)
    
    return integrity_results

def transform_silver_to_gold():
    """Main transformation function with validation"""
    
    logger.info("Starting Silver to Gold transformation with validation")
    
    spark = get_spark(app_name="MediTrack360-Gold-With-Validation")
    bucket = os.getenv("S3_BUCKET", "meditrack360-data-lake-6065273c")
    execution_date = os.getenv("EXECUTION_DATE", datetime.now().strftime('%Y-%m-%d'))
    
    try:
        # Your existing transformation code here...
        # After creating each table, validate it
        
        # Example after creating dim_patient:
        dim_patient = spark.read.parquet(get_gold_path("dim_patient"))
        patient_validation = validate_gold_table(dim_patient, "dim_patient")
        save_gold_validation_results(spark, patient_validation, "dim_patient", execution_date)
        
        # After creating fact_admissions:
        fact_admissions = spark.read.parquet(get_gold_path("fact_admissions"))
        admissions_validation = validate_gold_table(fact_admissions, "fact_admissions")
        save_gold_validation_results(spark, admissions_validation, "fact_admissions", execution_date)
        
        # Check referential integrity
        integrity_results = check_referential_integrity(spark, bucket, execution_date)
        
        # Log overall validation status
        if patient_validation['passed'] and admissions_validation['passed'] and integrity_results['overall_status'] == 'PASS':
            logger.info("✅ Gold transformation and validation completed successfully")
        else:
            logger.warning("⚠️ Gold transformation completed with validation issues")
        
        logger.info("Gold transformation completed with validation")
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    transform_silver_to_gold()