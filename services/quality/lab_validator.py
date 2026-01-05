"""
Lab Results Data Validator
Validates laboratory test results data
"""
import pandas as pd
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LabValidationResult:
    """Structured validation result for lab data"""
    source: str = "lab_results"
    validation_time: str = ""
    row_count: int = 0
    files_processed: int = 0
    critical_issues: List[str] = None
    warnings: List[str] = None
    test_type_stats: Dict[str, Any] = None
    passed: bool = False
    validation_summary: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.critical_issues is None:
            self.critical_issues = []
        if self.warnings is None:
            self.warnings = []
        if self.test_type_stats is None:
            self.test_type_stats = {}
        if self.validation_summary is None:
            self.validation_summary = {}
    
    def to_dict(self):
        return {
            'source': self.source,
            'validation_time': self.validation_time,
            'row_count': self.row_count,
            'files_processed': self.files_processed,
            'critical_issues': self.critical_issues,
            'warnings': self.warnings,
            'test_type_stats': self.test_type_stats,
            'passed': self.passed,
            'validation_summary': self.validation_summary
        }


class LabDataValidator:
    """Validator for laboratory test results"""
    
    # Reference ranges for common lab tests (can be extended)
    REFERENCE_RANGES = {
        'glucose': {'min': 70, 'max': 140, 'unit': 'mg/dL'},
        'cholesterol': {'min': 125, 'max': 200, 'unit': 'mg/dL'},
        'hemoglobin': {'min': 13.5, 'max': 17.5, 'unit': 'g/dL'},
        'wbc': {'min': 4.5, 'max': 11.0, 'unit': '10^3/μL'},
        'rbc': {'min': 4.7, 'max': 6.1, 'unit': '10^6/μL'},
        'platelets': {'min': 150, 'max': 450, 'unit': '10^3/μL'},
        'sodium': {'min': 135, 'max': 145, 'unit': 'mmol/L'},
        'potassium': {'min': 3.5, 'max': 5.0, 'unit': 'mmol/L'},
        'creatinine': {'min': 0.6, 'max': 1.2, 'unit': 'mg/dL'},
        'alt': {'min': 7, 'max': 56, 'unit': 'U/L'},
        'ast': {'min': 10, 'max': 40, 'unit': 'U/L'},
        'bilirubin': {'min': 0.1, 'max': 1.2, 'unit': 'mg/dL'}
    }
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
    
    def _default_config(self) -> Dict:
        """Default lab validation configuration"""
        return {
            'critical_issue_threshold': 0.05,  # 5% of tests
            'future_date_tolerance_days': 7,  # Tests up to 7 days in future
            'max_age_years': 10,  # Tests older than 10 years
            'outlier_sd_threshold': 4,  # 4 standard deviations for outliers
            'min_result_value': 0,  # Most lab values should be positive
            'test_type_consistency_threshold': 0.9,  # 90% consistency required
        }
    
    def validate_lab_results(self, df: pd.DataFrame, 
                           file_stats: Optional[List[Dict]] = None) -> LabValidationResult:
        """
        Validate laboratory test results
        
        Args:
            df: DataFrame containing lab test results
            file_stats: List of file statistics if multiple files combined
            
        Returns:
            LabValidationResult with validation details
        """
        result = LabValidationResult(
            validation_time=datetime.utcnow().isoformat(),
            row_count=len(df),
            files_processed=len(file_stats) if file_stats else 1
        )
        
        # Basic checks
        if self._is_empty(df):
            result.critical_issues.append('Lab results data is empty')
            return result
        
        # 1. Schema validation
        schema_issues = self._validate_schema(df)
        result.critical_issues.extend(schema_issues)
        
        # If schema is invalid, skip further validation
        if schema_issues:
            return result
        
        # 2. Data quality validation
        quality_issues, quality_warnings = self._validate_data_quality(df)
        result.critical_issues.extend(quality_issues)
        result.warnings.extend(quality_warnings)
        
        # 3. Business logic validation
        business_issues, business_warnings = self._validate_business_logic(df)
        result.critical_issues.extend(business_issues)
        result.warnings.extend(business_warnings)
        
        # 4. Test-specific validation
        test_issues, test_warnings, test_stats = self._validate_test_results(df)
        result.critical_issues.extend(test_issues)
        result.warnings.extend(test_warnings)
        result.test_type_stats = test_stats
        
        # 5. Calculate metrics and summary
        result.validation_summary = self._calculate_summary(df, result)
        
        # 6. Determine if validation passed
        result.passed = self._determine_validation_pass(
            result.critical_issues, 
            result.row_count
        )
        
        return result
    
    # ========== VALIDATION METHODS ==========
    
    def _is_empty(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame is empty"""
        if len(df) == 0:
            logger.error("Lab results data is empty")
            return True
        return False
    
    def _validate_schema(self, df: pd.DataFrame) -> List[str]:
        """Validate required columns exist"""
        issues = []
        
        required_columns = ['patient_id', 'test_type', 'test_date', 'result_value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")
        
        return issues
    
    def _validate_data_quality(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data quality rules"""
        issues = []
        warnings = []
        
        # 1. Patient ID validation
        patient_issues, patient_warnings = self._validate_patient_ids(df)
        issues.extend(patient_issues)
        warnings.extend(patient_warnings)
        
        # 2. Test type validation
        test_type_issues, test_type_warnings = self._validate_test_types(df)
        issues.extend(test_type_issues)
        warnings.extend(test_type_warnings)
        
        # 3. Test date validation
        date_issues, date_warnings = self._validate_test_dates(df)
        issues.extend(date_issues)
        warnings.extend(date_warnings)
        
        return issues, warnings
    
    def _validate_business_logic(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate business logic rules"""
        issues = []
        warnings = []
        
        # 1. Result value basic validation
        result_issues, result_warnings = self._validate_result_values(df)
        issues.extend(result_issues)
        warnings.extend(result_warnings)
        
        # 2. Temporal consistency (if multiple tests per patient)
        temporal_warnings = self._validate_temporal_consistency(df)
        warnings.extend(temporal_warnings)
        
        # 3. Test frequency validation
        frequency_warnings = self._validate_test_frequency(df)
        warnings.extend(frequency_warnings)
        
        return issues, warnings
    
    def _validate_patient_ids(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate patient IDs"""
        issues = []
        warnings = []
        
        # 1. Null patient IDs
        null_patient_ids = df['patient_id'].isnull().sum()
        if null_patient_ids > 0:
            issues.append(f"Found {null_patient_ids} tests without patient ID")
        
        # 2. Invalid patient ID format
        # Assuming patient IDs should be alphanumeric
        valid_format = df['patient_id'].astype(str).str.match(r'^[A-Za-z0-9\-_]+$')
        invalid_format = ~valid_format & df['patient_id'].notnull()
        
        if invalid_format.sum() > 0:
            warnings.append(f"Found {invalid_format.sum()} tests with unusual patient ID format")
        
        # 3. Very long/short patient IDs
        if 'patient_id' in df.columns:
            id_lengths = df['patient_id'].astype(str).str.len()
            unusual_length = ((id_lengths < 3) | (id_lengths > 20)).sum()
            
            if unusual_length > 0:
                warnings.append(f"Found {unusual_length} tests with unusual patient ID length")
        
        return issues, warnings
    
    def _validate_test_types(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate test types"""
        issues = []
        warnings = []
        
        # 1. Null test types
        null_test_types = df['test_type'].isnull().sum()
        if null_test_types > 0:
            issues.append(f"Found {null_test_types} tests without test type")
        
        # 2. Empty test types
        empty_test_types = df[df['test_type'].astype(str).str.strip() == '']
        if len(empty_test_types) > 0:
            issues.append(f"Found {len(empty_test_types)} tests with empty test type")
        
        # 3. Test type consistency
        test_type_counts = df['test_type'].value_counts()
        
        # Check for very rare test types (potential typos)
        rare_threshold = max(1, len(df) * 0.001)  # 0.1% threshold
        rare_tests = test_type_counts[test_type_counts < rare_threshold]
        
        if len(rare_tests) > 0:
            warnings.append(f"Found {len(rare_tests)} very rare test types (potential typos)")
        
        # 4. Standardize test type names (create standardized version)
        df['_test_type_std'] = df['test_type'].str.lower().str.strip()
        
        # Count standardized types
        std_test_type_counts = df['_test_type_std'].value_counts()
        result.test_type_stats['unique_test_types'] = len(std_test_type_counts)
        result.test_type_stats['test_type_distribution'] = std_test_type_counts.head(20).to_dict()
        
        return issues, warnings
    
    def _validate_test_dates(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate test dates"""
        issues = []
        warnings = []
        
        try:
            # Parse test dates
            df['_test_dt'] = pd.to_datetime(df['test_date'], errors='coerce')
            
            # 1. Invalid date formats
            invalid_dates = df[df['_test_dt'].isna()]
            if len(invalid_dates) > 0:
                issues.append(f"Found {len(invalid_dates)} tests with invalid date format")
            
            # 2. Future test dates
            now = pd.Timestamp.now()
            future_cutoff = now + timedelta(days=self.config['future_date_tolerance_days'])
            
            far_future = df[df['_test_dt'] > future_cutoff]
            near_future = df[(df['_test_dt'] > now) & (df['_test_dt'] <= future_cutoff)]
            
            if len(far_future) > 0:
                issues.append(f"Found {len(far_future)} tests with dates >{self.config['future_date_tolerance_days']} days in future")
            
            if len(near_future) > 0:
                warnings.append(f"Found {len(near_future)} tests with dates up to {self.config['future_date_tolerance_days']} days in future")
            
            # 3. Very old tests
            max_age = now - pd.DateOffset(years=self.config['max_age_years'])
            old_tests = df[df['_test_dt'] < max_age]
            
            if len(old_tests) > 0:
                warnings.append(f"Found {len(old_tests)} tests older than {self.config['max_age_years']} years")
            
            # 4. Calculate date statistics
            if '_test_dt' in df.columns and len(df[df['_test_dt'].notna()]) > 0:
                result.test_type_stats['latest_test_date'] = df['_test_dt'].max().isoformat()
                result.test_type_stats['earliest_test_date'] = df['_test_dt'].min().isoformat()
                
                # Tests per day
                tests_per_day = df['_test_dt'].dt.date.value_counts()
                result.test_type_stats['avg_tests_per_day'] = tests_per_day.mean()
        
        except Exception as e:
            issues.append(f"Failed to validate test dates: {str(e)}")
        
        return issues, warnings
    
    def _validate_result_values(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate result values"""
        issues = []
        warnings = []
        
        # 1. Null result values
        null_results = df['result_value'].isnull().sum()
        if null_results > 0:
            warnings.append(f"Found {null_results} tests with null results")
        
        # 2. Attempt to convert to numeric
        df['_result_numeric'] = pd.to_numeric(df['result_value'], errors='coerce')
        
        # 3. Non-numeric results (for tests that should be numeric)
        non_numeric = df[df['_result_numeric'].isna() & df['result_value'].notna()]
        if len(non_numeric) > 0:
            # Check if these are valid non-numeric results (like "positive", "negative")
            non_numeric_types = non_numeric['test_type'].unique()[:5]
            warnings.append(f"Found {len(non_numeric)} non-numeric results for test types: {list(non_numeric_types)}")
        
        # 4. Negative values for tests that should be positive
        if '_test_type_std' in df.columns:
            numeric_tests = df[df['_result_numeric'].notna()]
            
            # Tests that should always be positive
            positive_tests = numeric_tests[
                numeric_tests['_test_type_std'].str.contains('glucose|cholesterol|hemoglobin|wbc|rbc|platelets')
            ]
            
            negative_values = positive_tests[positive_tests['_result_numeric'] < self.config['min_result_value']]
            if len(negative_values) > 0:
                issues.append(f"Found {len(negative_values)} tests with negative values (should be positive)")
        
        return issues, warnings
    
    def _validate_temporal_consistency(self, df: pd.DataFrame) -> List[str]:
        """Validate temporal consistency of tests"""
        warnings = []
        
        if '_test_dt' in df.columns and 'patient_id' in df.columns:
            # Check for patients with multiple tests on same day
            patient_test_dates = df.groupby(['patient_id', '_test_dt']).size()
            multiple_same_day = patient_test_dates[patient_test_dates > 1]
            
            if len(multiple_same_day) > 0:
                warnings.append(f"Found {len(multiple_same_day)} patient-date combinations with multiple tests")
        
        return warnings
    
    def _validate_test_frequency(self, df: pd.DataFrame) -> List[str]:
        """Validate test frequency patterns"""
        warnings = []
        
        if '_test_dt' in df.columns and 'patient_id' in df.columns:
            # Calculate days between tests for each patient
            patient_tests = df.sort_values(['patient_id', '_test_dt'])
            patient_tests['_days_between'] = patient_tests.groupby('patient_id')['_test_dt'].diff().dt.days
            
            # Check for unusually frequent testing
            too_frequent = patient_tests[patient_tests['_days_between'] < 1]  # Same day or negative
            if len(too_frequent) > 0:
                warnings.append(f"Found {len(too_frequent)} tests with <1 day interval")
        
        return warnings
    
    def _validate_test_results(self, df: pd.DataFrame) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """Validate test-specific results using reference ranges"""
        issues = []
        warnings = []
        test_stats = {}
        
        if '_test_type_std' in df.columns and '_result_numeric' in df.columns:
            numeric_tests = df[df['_result_numeric'].notna()].copy()
            
            if len(numeric_tests) == 0:
                return issues, warnings, test_stats
            
            # Initialize test statistics
            test_stats['total_numeric_tests'] = len(numeric_tests)
            test_stats['abnormal_results'] = {}
            
            # Check each test type against reference ranges
            for test_name, ref_range in self.REFERENCE_RANGES.items():
                # Find matching tests (case-insensitive partial match)
                test_matches = numeric_tests[
                    numeric_tests['_test_type_std'].str.contains(test_name, case=False, na=False)
                ]
                
                if len(test_matches) > 0:
                    # Calculate statistics
                    test_values = test_matches['_result_numeric']
                    
                    test_stats[f'{test_name}_count'] = len(test_matches)
                    test_stats[f'{test_name}_mean'] = test_values.mean()
                    test_stats[f'{test_name}_std'] = test_values.std()
                    
                    # Check for values outside reference range
                    below_range = test_values[test_values < ref_range['min']]
                    above_range = test_values[test_values > ref_range['max']]
                    
                    total_abnormal = len(below_range) + len(above_range)
                    
                    if total_abnormal > 0:
                        abnormal_pct = total_abnormal / len(test_matches)
                        
                        if abnormal_pct > 0.5:  # More than 50% abnormal
                            issues.append(f"{test_name}: {total_abnormal} abnormal results ({abnormal_pct:.1%})")
                        else:
                            warnings.append(f"{test_name}: {total_abnormal} abnormal results ({abnormal_pct:.1%})")
                        
                        test_stats['abnormal_results'][test_name] = {
                            'below_range': len(below_range),
                            'above_range': len(above_range),
                            'total_abnormal': total_abnormal,
                            'abnormal_pct': abnormal_pct
                        }
                    
                    # Check for extreme outliers
                    if len(test_matches) > 10:  # Need enough data
                        mean = test_values.mean()
                        std = test_values.std()
                        
                        if std > 0:
                            outliers = test_values[
                                abs(test_values - mean) > self.config['outlier_sd_threshold'] * std
                            ]
                            
                            if len(outliers) > 0:
                                warnings.append(f"{test_name}: {len(outliers)} extreme outliers")
                
                # Remove matched tests to avoid double counting
                numeric_tests = numeric_tests[~numeric_tests['_test_type_std'].str.contains(test_name, case=False, na=False)]
            
            # Remaining unclassified numeric tests
            if len(numeric_tests) > 0:
                remaining_types = numeric_tests['_test_type_std'].unique()[:10]
                test_stats['unclassified_test_types'] = list(remaining_types)
        
        return issues, warnings, test_stats
    
    def _calculate_summary(self, df: pd.DataFrame, result: LabValidationResult) -> Dict[str, Any]:
        """Calculate validation summary"""
        summary = {}
        
        summary['total_tests'] = len(df)
        summary['unique_patients'] = df['patient_id'].nunique() if 'patient_id' in df.columns else 0
        summary['unique_test_types'] = df['test_type'].nunique() if 'test_type' in df.columns else 0
        
        # Date range if available
        if '_test_dt' in df.columns and len(df[df['_test_dt'].notna()]) > 0:
            summary['date_range'] = {
                'start': df['_test_dt'].min().isoformat(),
                'end': df['_test_dt'].max().isoformat()
            }
        
        # Result value statistics if available
        if '_result_numeric' in df.columns and len(df[df['_result_numeric'].notna()]) > 0:
            numeric_results = df[df['_result_numeric'].notna()]['_result_numeric']
            summary['result_statistics'] = {
                'mean': float(numeric_results.mean()),
                'median': float(numeric_results.median()),
                'std': float(numeric_results.std()),
                'min': float(numeric_results.min()),
                'max': float(numeric_results.max())
            }
        
        # Validation metrics
        summary['validation_metrics'] = {
            'critical_issues_count': len(result.critical_issues),
            'warnings_count': len(result.warnings),
            'validation_passed': result.passed,
            'data_quality_score': self._calculate_quality_score(result)
        }
        
        return summary
    
    def _calculate_quality_score(self, result: LabValidationResult) -> float:
        """Calculate data quality score (0-100)"""
        if result.row_count == 0:
            return 0.0
        
        # Start with perfect score
        score = 100.0
        
        # Deduct for critical issues
        issue_penalty = min(50.0, len(result.critical_issues) * 10.0)
        score -= issue_penalty
        
        # Deduct for warnings
        warning_penalty = min(20.0, len(result.warnings) * 2.0)
        score -= warning_penalty
        
        # Ensure score is between 0 and 100
        return max(0.0, min(100.0, score))
    
    def _determine_validation_pass(self, issues: List[str], row_count: int) -> bool:
        """Determine if validation passed based on issues and thresholds"""
        
        # If any critical issues, check threshold
        if issues:
            issue_ratio = len(issues) / max(row_count, 1)
            if issue_ratio > self.config['critical_issue_threshold']:
                return False
        
        return True