"""
Pharmacy Data Validator
Validates drug inventory data from pharmacy API
"""

import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PharmacyValidationResult:
    """Structured validation result for pharmacy data"""

    source: str = "pharmacy"
    validation_time: str = ""
    row_count: int = 0
    expired_drugs: int = 0
    critical_issues: List[str] = None
    warnings: List[str] = None
    passed: bool = False
    drug_categories: Dict[str, int] = None

    def __post_init__(self):
        if self.critical_issues is None:
            self.critical_issues = []
        if self.warnings is None:
            self.warnings = []
        if self.drug_categories is None:
            self.drug_categories = {}

    def to_dict(self):
        return {
            "source": self.source,
            "validation_time": self.validation_time,
            "row_count": self.row_count,
            "expired_drugs": self.expired_drugs,
            "critical_issues": self.critical_issues,
            "warnings": self.warnings,
            "passed": self.passed,
            "drug_categories": self.drug_categories,
        }


class PharmacyDataValidator:
    """Validator for pharmacy inventory data"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()

        # Known drug categories for validation
        self.drug_categories = [
            "antibiotic",
            "analgesic",
            "antiviral",
            "antifungal",
            "vaccine",
            "insulin",
            "chemotherapy",
            "anesthetic",
        ]

    def _default_config(self) -> Dict:
        """Default pharmacy validation configuration"""
        return {
            "critical_issue_threshold": 0.01,  # 1% of drugs
            "expired_drug_tolerance": 0,  # No expired drugs allowed
            "min_quantity": 0,  # No negative quantities
            "max_quantity": 10000,  # Maximum reasonable quantity
            "expiry_buffer_days": 30,  # Warn if expiring within 30 days
            "high_quantity_threshold": 1000,  # Flag unusually high quantities
        }

    def validate_drug_inventory(self, df: pd.DataFrame) -> PharmacyValidationResult:
        """
        Validate drug inventory data

        Args:
            df: DataFrame containing drug inventory data

        Returns:
            PharmacyValidationResult with validation details
        """
        result = PharmacyValidationResult(
            validation_time=datetime.utcnow().isoformat(), row_count=len(df)
        )

        # Basic checks
        if self._is_empty(df):
            result.critical_issues.append("Drug inventory data is empty")
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

        # 4. Calculate metrics
        metrics = self._calculate_metrics(df)
        result.expired_drugs = metrics.get("expired_drugs", 0)
        result.drug_categories = metrics.get("drug_categories", {})

        # 5. Determine if validation passed
        result.passed = self._determine_validation_pass(
            result.critical_issues, result.row_count, result.expired_drugs
        )

        return result

    # ========== VALIDATION METHODS ==========

    def _is_empty(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame is empty"""
        if len(df) == 0:
            logger.error("Drug inventory data is empty")
            return True
        return False

    def _validate_schema(self, df: pd.DataFrame) -> List[str]:
        """Validate required columns exist"""
        issues = []

        required_columns = ["drug_id", "drug_name", "quantity", "expiry_date"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")

        return issues

    def _validate_data_quality(self, df: pd.DataFrame) -> tuple:
        """Validate data quality rules"""
        issues = []
        warnings = []

        # 1. Duplicate drug IDs
        duplicate_ids = df["drug_id"].duplicated().sum()
        if duplicate_ids > 0:
            issues.append(f"Found {duplicate_ids} duplicate drug IDs")

        # 2. Null values in critical columns
        null_drug_ids = df["drug_id"].isnull().sum()
        if null_drug_ids > 0:
            issues.append(f"Found {null_drug_ids} null drug IDs")

        null_drug_names = df["drug_name"].isnull().sum()
        if null_drug_names > 0:
            issues.append(f"Found {null_drug_names} null drug names")

        # 3. Invalid drug names
        invalid_names = self._validate_drug_names(df)
        warnings.extend(invalid_names)

        return issues, warnings

    def _validate_business_logic(self, df: pd.DataFrame) -> tuple:
        """Validate business logic rules"""
        issues = []
        warnings = []

        # 1. Quantity validation
        quantity_issues, quantity_warnings = self._validate_quantities(df)
        issues.extend(quantity_issues)
        warnings.extend(quantity_warnings)

        # 2. Expiry date validation
        expiry_issues, expiry_warnings = self._validate_expiry_dates(df)
        issues.extend(expiry_issues)
        warnings.extend(expiry_warnings)

        # 3. Drug category validation
        category_warnings = self._validate_drug_categories(df)
        warnings.extend(category_warnings)

        return issues, warnings

    def _validate_drug_names(self, df: pd.DataFrame) -> List[str]:
        """Validate drug names"""
        warnings = []

        # Check for empty or whitespace-only names
        empty_names = df[
            df["drug_name"].isnull() | (df["drug_name"].astype(str).str.strip() == "")
        ]
        if len(empty_names) > 0:
            warnings.append(f"Found {len(empty_names)} empty drug names")

        # Check for unusually short names
        short_names = df[df["drug_name"].astype(str).str.len() < 2]
        if len(short_names) > 0:
            warnings.append(
                f"Found {len(short_names)} very short drug names (< 2 chars)"
            )

        # Check for numeric-only names (potential data entry error)
        numeric_names = df[df["drug_name"].astype(str).str.match(r"^\d+$")]
        if len(numeric_names) > 0:
            warnings.append(
                f"Found {len(numeric_names)} drug names that are only numbers"
            )

        return warnings

    def _validate_quantities(self, df: pd.DataFrame) -> tuple:
        """Validate drug quantities"""
        issues = []
        warnings = []

        # Convert to numeric, coerce errors
        df["_quantity_numeric"] = pd.to_numeric(df["quantity"], errors="coerce")

        # 1. Negative quantities
        negative_qty = df[df["_quantity_numeric"] < self.config["min_quantity"]]
        if len(negative_qty) > 0:
            issues.append(f"Found {len(negative_qty)} drugs with negative quantity")

        # 2. Unusually high quantities
        high_qty = df[df["_quantity_numeric"] > self.config["high_quantity_threshold"]]
        if len(high_qty) > 0:
            warnings.append(
                f"Found {len(high_qty)} drugs with unusually high quantity (> {self.config['high_quantity_threshold']})"
            )

        # 3. Maximum quantity check
        max_qty = df[df["_quantity_numeric"] > self.config["max_quantity"]]
        if len(max_qty) > 0:
            issues.append(
                f"Found {len(max_qty)} drugs exceeding maximum quantity ({self.config['max_quantity']})"
            )

        # 4. Zero quantity warning
        zero_qty = df[df["_quantity_numeric"] == 0]
        if len(zero_qty) > 0:
            warnings.append(
                f"Found {len(zero_qty)} drugs with zero quantity (may be out of stock)"
            )

        # 5. Non-numeric quantities
        non_numeric = df[df["_quantity_numeric"].isna()]
        if len(non_numeric) > 0:
            issues.append(
                f"Found {len(non_numeric)} drugs with non-numeric quantity values"
            )

        return issues, warnings

    def _validate_expiry_dates(self, df: pd.DataFrame) -> tuple:
        """Validate drug expiry dates"""
        issues = []
        warnings = []

        try:
            # Parse expiry dates
            df["_expiry_dt"] = pd.to_datetime(df["expiry_date"], errors="coerce")

            # 1. Invalid date formats
            invalid_dates = df[df["_expiry_dt"].isna()]
            if len(invalid_dates) > 0:
                issues.append(
                    f"Found {len(invalid_dates)} drugs with invalid expiry date format"
                )

            # 2. Already expired drugs
            now = pd.Timestamp.now()
            expired = df[df["_expiry_dt"] < now]
            result.expired_drugs = len(expired)

            if len(expired) > self.config["expired_drug_tolerance"]:
                issues.append(
                    f"Found {len(expired)} expired drugs (tolerance: {self.config['expired_drug_tolerance']})"
                )

            # 3. Drugs expiring soon (within buffer period)
            buffer_date = now + timedelta(days=self.config["expiry_buffer_days"])
            expiring_soon = df[
                (df["_expiry_dt"] >= now) & (df["_expiry_dt"] <= buffer_date)
            ]

            if len(expiring_soon) > 0:
                warnings.append(
                    f"Found {len(expiring_soon)} drugs expiring within {self.config['expiry_buffer_days']} days"
                )

            # 4. Future dates too far in future
            max_future_date = now + timedelta(days=365 * 5)  # 5 years
            far_future = df[df["_expiry_dt"] > max_future_date]

            if len(far_future) > 0:
                warnings.append(
                    f"Found {len(far_future)} drugs with expiry dates >5 years in future"
                )

            # 5. Past dates (before 2000)
            ancient_date = pd.Timestamp("2000-01-01")
            ancient_drugs = df[df["_expiry_dt"] < ancient_date]

            if len(ancient_drugs) > 0:
                issues.append(
                    f"Found {len(ancient_drugs)} drugs with expiry dates before 2000"
                )

        except Exception as e:
            issues.append(f"Failed to validate expiry dates: {str(e)}")

        return issues, warnings

    def _validate_drug_categories(self, df: pd.DataFrame) -> List[str]:
        """Validate drug categories if present"""
        warnings = []

        if "category" in df.columns:
            # Check for unknown categories
            known_cats = set(self.drug_categories)
            df_cats = set(df["category"].dropna().unique())
            unknown_cats = df_cats - known_cats

            if unknown_cats:
                warnings.append(
                    f"Found unknown drug categories: {list(unknown_cats)[:5]}"
                )

            # Check for empty categories
            empty_cats = df[
                df["category"].isnull() | (df["category"].astype(str).str.strip() == "")
            ]
            if len(empty_cats) > 0:
                warnings.append(f"Found {len(empty_cats)} drugs without category")

        return warnings

    def _calculate_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate pharmacy metrics"""
        metrics = {}

        metrics["total_drugs"] = len(df)

        # Calculate expired drugs
        if "_expiry_dt" in df.columns:
            expired = df[df["_expiry_dt"] < pd.Timestamp.now()]
            metrics["expired_drugs"] = len(expired)

        # Calculate quantity statistics
        if "_quantity_numeric" in df.columns:
            metrics["total_quantity"] = df["_quantity_numeric"].sum()
            metrics["avg_quantity"] = df["_quantity_numeric"].mean()

        # Drug categories
        if "category" in df.columns:
            category_counts = df["category"].value_counts().to_dict()
            metrics["drug_categories"] = category_counts

        # Supplier metrics if available
        if "supplier" in df.columns:
            supplier_counts = df["supplier"].value_counts().head(5).to_dict()
            metrics["top_suppliers"] = supplier_counts

        return metrics

    def _determine_validation_pass(
        self, issues: List[str], row_count: int, expired_drugs: int
    ) -> bool:
        """Determine if validation passed based on issues and thresholds"""

        # If any critical issues, fail
        if issues:
            # Check if issues exceed threshold
            issue_ratio = len(issues) / max(row_count, 1)
            if issue_ratio > self.config["critical_issue_threshold"]:
                return False

            # Check expired drugs tolerance
            expired_ratio = expired_drugs / max(row_count, 1)
            if expired_ratio > self.config["expired_drug_tolerance"] / 100:
                return False

        return True
