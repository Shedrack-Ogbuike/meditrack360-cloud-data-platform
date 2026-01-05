"""
PostgreSQL Data Validator
Validates patient, admission, discharge, and ward data from Postgres
"""
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Structured validation result"""
    table_name: str
    validation_time: str
    row_count: int
    issues: List[str]
    warnings: List[str]
    passed: bool
    validation_metrics: Dict[str, Any]
    
    def to_dict(self):
        return {
            'table_name': self.table_name,
            'validation_time': self.validation_time,
            'row_count': self.row_count,
            'issues': self.issues,
            'warnings': self.warnings,
            'passed': self.passed,
            'validation_metrics': self.validation_metrics
        }


class PostgresDataValidator:
    """Validator for PostgreSQL healthcare data"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
    
    def _default_config(self) -> Dict:
        """Default validation configuration"""
        return {
            'critical_issue_threshold': 0.05,  # 5% of rows
            'warning_threshold': 0.10,  # 10% of rows
            'max_future_years': 1,  # Max years in future for dates
            'min_patient_age': 0,
            'max_patient_age': 120,
            'min_ward_capacity': 1,
            'max_ward_capacity': 200
        }
    
    def validate_patients(self, df: pd.DataFrame) -> ValidationResult:
        """Validate patients table"""
        issues = []
        warnings = []
        metrics = {}
        
        # Basic checks
        if self._is_empty(df, 'patients'):
            issues.append('Table is empty')
            return self._create_result('patients', df, issues, warnings, metrics)
        
        # 1. Schema validation
        required_columns = ['patient_id', 'first_name', 'last_name', 'date_of_birth']
        schema_issues = self._validate_schema(df, required_columns)
        issues.extend(schema_issues)
        
        # 2. Primary key validation
        if 'patient_id' in df.columns:
            pk_issues = self._validate_primary_key(df, 'patient_id')
            issues.extend(pk_issues)
            
            # Check patient_id format
            format_issues = self._validate_patient_id_format(df['patient_id'])
            issues.extend(format_issues)
        
        # 3. Data quality validation
        if 'date_of_birth' in df.columns:
            date_issues, date_warnings = self._validate_patient_dates(df)
            issues.extend(date_issues)
            warnings.extend(date_warnings)
            
            # Age calculation validation
            if not any(issue.startswith('date_of_birth') for issue in issues):
                age_warnings = self._validate_patient_ages(df)
                warnings.extend(age_warnings)
        
        # 4. Name validation
        if 'first_name' in df.columns:
            name_warnings = self._validate_names(df, 'first_name')
            warnings.extend(name_warnings)
        
        if 'last_name' in df.columns:
            name_warnings = self._validate_names(df, 'last_name')
            warnings.extend(name_warnings)
        
        # 5. Calculate metrics
        metrics.update(self._calculate_patient_metrics(df))
        
        return self._create_result('patients', df, issues, warnings, metrics)
    
    def validate_admissions(self, df: pd.DataFrame) -> ValidationResult:
        """Validate admissions table"""
        issues = []
        warnings = []
        metrics = {}
        
        if self._is_empty(df, 'admissions'):
            issues.append('Table is empty')
            return self._create_result('admissions', df, issues, warnings, metrics)
        
        # 1. Schema validation
        required_columns = ['admission_id', 'patient_id', 'admission_date', 'ward_id']
        schema_issues = self._validate_schema(df, required_columns)
        issues.extend(schema_issues)
        
        # 2. Primary key validation
        if 'admission_id' in df.columns:
            pk_issues = self._validate_primary_key(df, 'admission_id')
            issues.extend(pk_issues)
        
        # 3. Foreign key validation (patient_id should exist in patients table)
        # Note: This requires patients data - could be done in separate validation
        if 'patient_id' in df.columns:
            null_patients = df['patient_id'].isnull().sum()
            if null_patients > 0:
                issues.append(f'{null_patients} admissions missing patient_id')
        
        # 4. Date validation
        if 'admission_date' in df.columns:
            date_issues, date_warnings = self._validate_admission_dates(df)
            issues.extend(date_issues)
            warnings.extend(date_warnings)
        
        # 5. Calculate metrics
        metrics.update(self._calculate_admission_metrics(df))
        
        return self._create_result('admissions', df, issues, warnings, metrics)
    
    def validate_discharges(self, df: pd.DataFrame) -> ValidationResult:
        """Validate discharges table"""
        issues = []
        warnings = []
        metrics = {}
        
        if self._is_empty(df, 'discharges'):
            issues.append('Table is empty')
            return self._create_result('discharges', df, issues, warnings, metrics)
        
        # 1. Schema validation
        required_columns = ['discharge_id', 'patient_id', 'discharge_date']
        schema_issues = self._validate_schema(df, required_columns)
        issues.extend(schema_issues)
        
        # 2. Primary key validation
        if 'discharge_id' in df.columns:
            pk_issues = self._validate_primary_key(df, 'discharge_id')
            issues.extend(pk_issues)
        
        # 3. Date validation
        if 'discharge_date' in df.columns:
            date_issues = self._validate_discharge_dates(df)
            issues.extend(date_issues)
        
        return self._create_result('discharges', df, issues, warnings, metrics)
    
    def validate_wards(self, df: pd.DataFrame) -> ValidationResult:
        """Validate wards table"""
        issues = []
        warnings = []
        metrics = {}
        
        if self._is_empty(df, 'wards'):
            issues.append('Table is empty')
            return self._create_result('wards', df, issues, warnings, metrics)
        
        # 1. Schema validation
        required_columns = ['ward_id', 'ward_name']
        schema_issues = self._validate_schema(df, required_columns)
        issues.extend(schema_issues)
        
        # 2. Primary key validation
        if 'ward_id' in df.columns:
            pk_issues = self._validate_primary_key(df, 'ward_id')
            issues.extend(pk_issues)
        
        # 3. Ward capacity validation
        if 'capacity' in df.columns:
            capacity_issues = self._validate_ward_capacity(df)
            issues.extend(capacity_issues)
        
        return self._create_result('wards', df, issues, warnings, metrics)
    
    # ========== HELPER METHODS ==========
    
    def _is_empty(self, df: pd.DataFrame, table_name: str) -> bool:
        """Check if DataFrame is empty"""
        if len(df) == 0:
            logger.warning(f"Table {table_name} is empty")
            return True
        return False
    
    def _validate_schema(self, df: pd.DataFrame, required_columns: List[str]) -> List[str]:
        """Validate required columns exist"""
        issues = []
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")
        
        return issues
    
    def _validate_primary_key(self, df: pd.DataFrame, pk_column: str) -> List[str]:
        """Validate primary key uniqueness and non-null"""
        issues = []
        
        # Check for nulls
        null_count = df[pk_column].isnull().sum()
        if null_count > 0:
            issues.append(f"{null_count} null values in primary key {pk_column}")
        
        # Check for duplicates
        duplicate_count = df[pk_column].duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"{duplicate_count} duplicate values in primary key {pk_column}")
        
        return issues
    
    def _validate_patient_id_format(self, patient_ids: pd.Series) -> List[str]:
        """Validate patient ID format"""
        issues = []
        # Assuming patient IDs are alphanumeric and not empty
        invalid_ids = patient_ids[~patient_ids.astype(str).str.match(r'^[A-Za-z0-9\-_]+$')]
        
        if len(invalid_ids) > 0:
            issues.append(f"{len(invalid_ids)} patient IDs with invalid format")
        
        return issues
    
    def _validate_patient_dates(self, df: pd.DataFrame) -> tuple:
        """Validate patient dates (birth dates)"""
        issues = []
        warnings = []
        
        try:
            # Convert to datetime
            df['_dob_temp'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
            
            # Check for parsing failures
            invalid_dates = df['_dob_temp'].isna().sum()
            if invalid_dates > 0:
                issues.append(f"{invalid_dates} invalid date formats in date_of_birth")
            
            # Check for future birth dates (critical issue)
            now = pd.Timestamp.now()
            max_future = now + pd.DateOffset(years=self.config['max_future_years'])
            future_births = df[df['_dob_temp'] > now]
            
            if len(future_births) > 0:
                # Future birth dates within allowed range (e.g., scheduled births)
                reasonable_future = future_births[future_births['_dob_temp'] <= max_future]
                unreasonable_future = future_births[future_births['_dob_temp'] > max_future]
                
                if len(unreasonable_future) > 0:
                    issues.append(f"{len(unreasonable_future)} patients with birth dates >{self.config['max_future_years']} years in future")
                
                if len(reasonable_future) > 0:
                    warnings.append(f"{len(reasonable_future)} patients with future birth dates (within {self.config['max_future_years']} years)")
            
            # Check for very old patients (warning)
            oldest_reasonable = now - pd.DateOffset(years=120)
            very_old = df[df['_dob_temp'] < oldest_reasonable]
            
            if len(very_old) > 0:
                warnings.append(f"{len(very_old)} patients older than 120 years")
        
        except Exception as e:
            issues.append(f"Failed to validate dates: {str(e)}")
        
        return issues, warnings
    
    def _validate_patient_ages(self, df: pd.DataFrame) -> List[str]:
        """Calculate and validate patient ages"""
        warnings = []
        
        try:
            df['_age_temp'] = (pd.Timestamp.now() - df['_dob_temp']).dt.days / 365.25
            
            # Check age ranges
            too_young = df[df['_age_temp'] < self.config['min_patient_age']]
            too_old = df[df['_age_temp'] > self.config['max_patient_age']]
            
            if len(too_young) > 0:
                warnings.append(f"{len(too_young)} patients with age < {self.config['min_patient_age']}")
            
            if len(too_old) > 0:
                warnings.append(f"{len(too_old)} patients with age > {self.config['max_patient_age']}")
        
        except Exception as e:
            warnings.append(f"Could not calculate ages: {str(e)}")
        
        return warnings
    
    def _validate_names(self, df: pd.DataFrame, column: str) -> List[str]:
        """Validate name columns"""
        warnings = []
        
        # Check for empty names
        empty_names = df[df[column].isnull() | (df[column].astype(str).str.strip() == '')]
        
        if len(empty_names) > 0:
            warnings.append(f"{len(empty_names)} empty {column} values")
        
        # Check for unusual characters
        unusual_names = df[df[column].astype(str).str.contains(r'[0-9@#$%^&*]', na=False)]
        
        if len(unusual_names) > 0:
            warnings.append(f"{len(unusual_names)} {column} values with unusual characters")
        
        return warnings
    
    def _validate_admission_dates(self, df: pd.DataFrame) -> tuple:
        """Validate admission dates"""
        issues = []
        warnings = []
        
        try:
            df['_admission_dt'] = pd.to_datetime(df['admission_date'], errors='coerce')
            
            # Invalid dates
            invalid_dates = df['_admission_dt'].isna().sum()
            if invalid_dates > 0:
                issues.append(f"{invalid_dates} invalid admission dates")
            
            # Future admissions (could be scheduled)
            future_admissions = df[df['_admission_dt'] > pd.Timestamp.now()]
            if len(future_admissions) > 0:
                warnings.append(f"{len(future_admissions)} future admission dates")
            
            # Very old admissions
            two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
            old_admissions = df[df['_admission_dt'] < two_years_ago]
            if len(old_admissions) > 0:
                warnings.append(f"{len(old_admissions)} admissions older than 2 years")
        
        except Exception as e:
            issues.append(f"Failed to validate admission dates: {str(e)}")
        
        return issues, warnings
    
    def _validate_discharge_dates(self, df: pd.DataFrame) -> List[str]:
        """Validate discharge dates"""
        issues = []
        
        try:
            df['_discharge_dt'] = pd.to_datetime(df['discharge_date'], errors='coerce')
            
            invalid_dates = df['_discharge_dt'].isna().sum()
            if invalid_dates > 0:
                issues.append(f"{invalid_dates} invalid discharge dates")
        
        except Exception as e:
            issues.append(f"Failed to validate discharge dates: {str(e)}")
        
        return issues
    
    def _validate_ward_capacity(self, df: pd.DataFrame) -> List[str]:
        """Validate ward capacity values"""
        issues = []
        
        # Negative capacity
        negative_capacity = df[df['capacity'] < 0]
        if len(negative_capacity) > 0:
            issues.append(f"{len(negative_capacity)} wards with negative capacity")
        
        # Unrealistic capacity
        unrealistic_capacity = df[
            (df['capacity'] < self.config['min_ward_capacity']) | 
            (df['capacity'] > self.config['max_ward_capacity'])
        ]
        if len(unrealistic_capacity) > 0:
            issues.append(f"{len(unrealistic_capacity)} wards with unrealistic capacity")
        
        return issues
    
    def _calculate_patient_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate patient metrics"""
        metrics = {}
        
        metrics['total_patients'] = len(df)
        
        if 'gender' in df.columns:
            gender_counts = df['gender'].value_counts().to_dict()
            metrics['gender_distribution'] = gender_counts
        
        if 'date_of_birth' in df.columns and '_dob_temp' in df.columns:
            try:
                avg_age = (pd.Timestamp.now() - df['_dob_temp']).dt.days.mean() / 365.25
                metrics['average_age'] = round(avg_age, 2)
            except:
                pass
        
        return metrics
    
    def _calculate_admission_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate admission metrics"""
        metrics = {}
        
        metrics['total_admissions'] = len(df)
        
        if '_admission_dt' in df.columns:
            metrics['latest_admission'] = df['_admission_dt'].max().isoformat()
            metrics['earliest_admission'] = df['_admission_dt'].min().isoformat()
        
        if 'ward_id' in df.columns:
            ward_counts = df['ward_id'].value_counts().head(10).to_dict()
            metrics['top_wards'] = ward_counts
        
        return metrics
    
    def _create_result(self, table_name: str, df: pd.DataFrame, 
                      issues: List[str], warnings: List[str], 
                      metrics: Dict[str, Any]) -> ValidationResult:
        """Create validation result object"""
        
        # Determine if validation passed
        passed = len(issues) == 0
        
        # Check if issues exceed threshold
        if len(df) > 0:
            issue_ratio = len(issues) / len(df)
            if issue_ratio > self.config['critical_issue_threshold']:
                issues.append(f"Issue ratio ({issue_ratio:.1%}) exceeds threshold ({self.config['critical_issue_threshold']:.1%})")
                passed = False
        
        return ValidationResult(
            table_name=table_name,
            validation_time=datetime.utcnow().isoformat(),
            row_count=len(df),
            issues=issues,
            warnings=warnings,
            passed=passed,
            validation_metrics=metrics
        )