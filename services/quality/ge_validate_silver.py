"""
Bronze to Silver transformation with validation
"""
import os
from pyspark.sql import SparkSession, functions as F, types as T
import json
from datetime import datetime

def validate_silver_table(df, table_name):
    """Validate silver table data quality"""
    validation_results = {
        'table': table_name,
        'validation_time': datetime.utcnow().isoformat(),
        'row_count': df.count(),
        'issues': [],
        'passed': True
    }
    
    # Table-specific validations
    if table_name == 'patients':
        # Check for null patient_ids
        null_patients = df.filter(F.col('patient_id').isNull()).count()
        if null_patients > 0:
            validation_results['issues'].append(f'{null_patients} null patient_ids')
            validation_results['passed'] = False
        
        # Check for duplicate patient_ids
        duplicate_patients = df.groupBy('patient_id').count().filter(F.col('count') > 1).count()
        if duplicate_patients > 0:
            validation_results['issues'].append(f'{duplicate_patients} duplicate patient_ids')
            validation_results['passed'] = False
        
        # Check for future birth dates
        if 'date_of_birth' in df.columns:
            future_births = df.filter(F.col('date_of_birth') > F.current_date()).count()
            if future_births > 0:
                validation_results['issues'].append(f'{future_births} future birth dates')
                validation_results['passed'] = False
    
    elif table_name == 'admissions':
        # Check for null admission_ids
        null_admissions = df.filter(F.col('admission_id').isNull()).count()
        if null_admissions > 0:
            validation_results['issues'].append(f'{null_admissions} null admission_ids')
            validation_results['passed'] = False
        
        # Check for future admission dates
        if 'admission_date' in df.columns:
            future_admissions = df.filter(F.col('admission_date') > F.current_date()).count()
            if future_admissions > 0:
                validation_results['warnings'] = [f'{future_admissions} future admission dates (scheduled?)']
    
    elif table_name == 'pharmacy_inventory':
        # Check for negative stock
        if 'stock_on_hand' in df.columns:
            negative_stock = df.filter(F.col('stock_on_hand') < 0).count()
            if negative_stock > 0:
                validation_results['issues'].append(f'{negative_stock} negative stock values')
                validation_results['passed'] = False
        
        # Check for expired drugs
        if 'expiry_date' in df.columns:
            expired_drugs = df.filter(F.col('expiry_date') < F.current_date()).count()
            if expired_drugs > 0:
                validation_results['issues'].append(f'{expired_drugs} expired drugs')
                validation_results['passed'] = False
    
    elif table_name == 'lab_results':
        # Check for null test types
        null_tests = df.filter(F.col('test_type').isNull()).count()
        if null_tests > 0:
            validation_results['issues'].append(f'{null_tests} null test types')
            validation_results['passed'] = False
        
        # Check for future test dates
        if 'sample_datetime' in df.columns:
            future_tests = df.filter(F.col('sample_datetime') > F.current_timestamp()).count()
            if future_tests > 0:
                validation_results['issues'].append(f'{future_tests} future test dates')
                validation_results['passed'] = False
    
    return validation_results

def save_validation_results(spark, validation_results, table_name, execution_date):
    """Save validation results to S3"""
    bucket = os.getenv("S3_BUCKET", "meditrack360-data-lake-6065273c")
    
    # Convert to DataFrame and save
    validation_df = spark.createDataFrame([validation_results])
    
    validation_path = f"s3a://{bucket}/silver/validation/{execution_date}/{table_name}_validation"
    validation_df.write.mode('append').json(validation_path)
    
    return validation_path

def main():
    # ... [Your existing main function code] ...
    
    # After writing each table, validate it
    execution_date = os.getenv('EXECUTION_DATE', datetime.now().strftime('%Y-%m-%d'))
    
    # Validate patients
    patients_validation = validate_silver_table(patients, 'patients')
    save_validation_results(spark, patients_validation, 'patients', execution_date)
    
    # Validate admissions
    admissions_validation = validate_silver_table(admissions, 'admissions')
    save_validation_results(spark, admissions_validation, 'admissions', execution_date)
    
    # Validate pharmacy
    pharmacy_validation = validate_silver_table(pharmacy, 'pharmacy_inventory')
    save_validation_results(spark, pharmacy_validation, 'pharmacy_inventory', execution_date)
    
    # Validate lab results
    labs_validation = validate_silver_table(labs, 'lab_results')
    save_validation_results(spark, labs_validation, 'lab_results', execution_date)
    
    # Log validation summary
    all_validations = [patients_validation, admissions_validation, pharmacy_validation, labs_validation]
    all_passed = all(v.get('passed', False) for v in all_validations)
    
    if all_passed:
        print("✅ All silver tables validated successfully")
    else:
        failed_tables = [v['table'] for v in all_validations if not v.get('passed', True)]
        print(f"⚠️ Silver validation failed for tables: {failed_tables}")
    
    spark.stop()