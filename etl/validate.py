# ============================================================
# FILE: etl/validate.py
# PROJECT: Global Risk Intelligence Platform
# AUTHOR: Pranoydas Eranadath
# DAY: 4-5 of Week 1
# PURPOSE: Data quality validation before loading to warehouse
# ============================================================

# ============================================================
# WHY DATA VALIDATION?
# Imagine you're building dashboards for a hospital.
# Bad data → wrong dashboard → wrong decision → patient harmed.
#
# In real DE jobs, bad data is your responsibility.
# "Garbage in, garbage out" — if your pipeline loads wrong data,
# every downstream report, ML model, and dashboard is wrong.
#
# This file validates data BEFORE it hits the warehouse.
# If checks fail → pipeline stops → alert sent → engineer fixes it.
#
# In Month 2 we use Great Expectations (professional library).
# This file teaches you the CONCEPTS using plain Python first.
# ============================================================

import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ============================================================
# VALIDATION RESULT CLASS
# Instead of just True/False, we return a detailed object
# that tells us WHAT failed and WHY
# This is good software design — informative error messages
# ============================================================

class ValidationResult:
    """
    Stores the result of a single validation check.

    Attributes:
        check_name: what we checked (e.g. 'no_null_iso_codes')
        passed: True if check passed, False if failed
        message: human-readable description
        details: additional info (e.g. how many rows failed)
    """

    def __init__(self, check_name, passed, message, details=None):
        self.check_name = check_name
        self.passed = passed
        self.message = message
        self.details = details or {}

    def __str__(self):
        status = "PASS ✓" if self.passed else "FAIL ✗"
        return f"[{status}] {self.check_name}: {self.message}"


# ============================================================
# INDIVIDUAL VALIDATION FUNCTIONS
# Each function checks ONE thing and returns a ValidationResult
# This makes it easy to add/remove checks and see exactly what fails
# ============================================================

def check_not_empty(df, min_rows=100):
    """
    Check 1: DataFrame must have at least min_rows rows.
    If we got 0 or very few rows, something went wrong in extraction.
    """
    row_count = len(df)
    passed = row_count >= min_rows

    return ValidationResult(
        check_name='not_empty',
        passed=passed,
        message=f"DataFrame has {row_count:,} rows (minimum: {min_rows:,})",
        details={'row_count': row_count, 'min_rows': min_rows}
    )


def check_required_columns(df, required_cols):
    """
    Check 2: All required columns must be present.
    If a column is missing, downstream code will crash.
    Catching this early gives a clear error message.
    """
    missing = [col for col in required_cols if col not in df.columns]
    passed = len(missing) == 0

    return ValidationResult(
        check_name='required_columns',
        passed=passed,
        message=f"Missing columns: {missing}" if missing else "All required columns present",
        details={'missing_columns': missing, 'present_columns': list(df.columns)}
    )


def check_null_rate(df, column, max_null_rate=0.05):
    """
    Check 3: Null values in a column must be below max_null_rate.
    Some nulls are okay (optional fields).
    But if 50% of country codes are missing, something is wrong.

    max_null_rate=0.05 means max 5% nulls allowed.
    """
    if column not in df.columns:
        return ValidationResult(
            check_name=f'null_rate_{column}',
            passed=False,
            message=f"Column '{column}' does not exist"
        )

    null_count = df[column].isnull().sum()
    null_rate = null_count / len(df) if len(df) > 0 else 1.0
    passed = null_rate <= max_null_rate

    return ValidationResult(
        check_name=f'null_rate_{column}',
        passed=passed,
        message=f"Null rate: {null_rate:.1%} (max allowed: {max_null_rate:.1%})",
        details={
            'null_count': int(null_count),
            'null_rate': float(null_rate),
            'max_allowed': max_null_rate
        }
    )


def check_no_negatives(df, column):
    """
    Check 4: Numeric column should have no negative values.
    Case counts and death counts cannot be negative in raw data.
    (Some reporting corrections cause negatives — we filter those out)
    """
    if column not in df.columns:
        return ValidationResult(
            check_name=f'no_negatives_{column}',
            passed=False,
            message=f"Column '{column}' does not exist"
        )

    negative_count = (df[column] < 0).sum()
    passed = negative_count == 0

    return ValidationResult(
        check_name=f'no_negatives_{column}',
        passed=passed,
        message=f"{negative_count:,} negative values found" if not passed else "No negatives found",
        details={
            'negative_count': int(negative_count),
            'negative_rows': df[df[column] < 0].head(5).to_dict() if not passed else {}
        }
    )


def check_date_range(df, date_column, max_future_days=1):
    """
    Check 5: Dates should not be unreasonably far in the future.
    Future dates usually mean data entry errors.
    We allow 1 day of buffer for timezone differences.
    """
    if date_column not in df.columns:
        return ValidationResult(
            check_name='date_range',
            passed=False,
            message=f"Column '{date_column}' does not exist"
        )

    max_allowed = pd.Timestamp.now() + pd.Timedelta(days=max_future_days)

    # Convert to datetime if not already
    dates = pd.to_datetime(df[date_column])
    future_dates = dates[dates > max_allowed]

    passed = len(future_dates) == 0

    return ValidationResult(
        check_name='date_range',
        passed=passed,
        message=f"{len(future_dates):,} future dates found" if not passed else f"Dates OK. Range: {dates.min().date()} to {dates.max().date()}",
        details={
            'min_date': str(dates.min().date()),
            'max_date': str(dates.max().date()),
            'future_count': len(future_dates)
        }
    )


def check_iso_code_format(df, column='iso_code'):
    """
    Check 6: ISO country codes must be exactly 3 uppercase letters.
    e.g. 'IND', 'USA', 'DEU' are valid
    'IN', 'INDIA', 'ind' are invalid
    """
    if column not in df.columns:
        return ValidationResult(
            check_name='iso_code_format',
            passed=False,
            message=f"Column '{column}' does not exist"
        )

    # Regex pattern: ^ start, [A-Z] uppercase letter, {3} exactly 3, $ end
    valid_pattern = df[column].str.match(r'^[A-Z]{3}$', na=False)
    invalid_count = (~valid_pattern).sum()
    passed = invalid_count == 0

    return ValidationResult(
        check_name='iso_code_format',
        passed=passed,
        message=f"{invalid_count:,} invalid ISO codes found" if not passed else "All ISO codes valid",
        details={'invalid_count': int(invalid_count)}
    )


def check_unique_combinations(df, columns, max_duplicate_rate=0.01):
    """
    Check 7: Key combinations should be mostly unique.
    For health data, the combination of (date, country, disease)
    should appear only once — we can't have two different case counts
    for India on the same day for the same disease.
    """
    if not all(col in df.columns for col in columns):
        return ValidationResult(
            check_name='unique_combinations',
            passed=False,
            message=f"Some columns not found: {columns}"
        )

    # duplicated() returns True for duplicate rows (keeps first occurrence)
    duplicate_count = df.duplicated(subset=columns).sum()
    duplicate_rate = duplicate_count / len(df) if len(df) > 0 else 0

    passed = duplicate_rate <= max_duplicate_rate

    return ValidationResult(
        check_name='unique_combinations',
        passed=passed,
        message=f"{duplicate_count:,} duplicate rows ({duplicate_rate:.1%})" if not passed else f"Uniqueness OK: {duplicate_count:,} duplicates ({duplicate_rate:.1%})",
        details={
            'duplicate_count': int(duplicate_count),
            'duplicate_rate': float(duplicate_rate)
        }
    )


def check_country_coverage(df, min_countries=50):
    """
    Check 8: We should have data for at least min_countries countries.
    If we only have data for 5 countries, something went wrong in ingestion.
    """
    if 'iso_code' not in df.columns:
        return ValidationResult(
            check_name='country_coverage',
            passed=False,
            message="Column 'iso_code' does not exist"
        )

    country_count = df['iso_code'].nunique()
    passed = country_count >= min_countries

    return ValidationResult(
        check_name='country_coverage',
        passed=passed,
        message=f"{country_count} countries found (minimum: {min_countries})",
        details={'country_count': country_count, 'min_required': min_countries}
    )


# ============================================================
# MAIN VALIDATION RUNNER
# Runs all checks and produces a summary report
# Returns True if ALL checks pass, False if any fail
# ============================================================

def validate_health_data(df):
    """
    Runs all validation checks on the health dataset.

    This is called BETWEEN transform and load.
    If validation fails, the pipeline stops and data is NOT loaded.

    Args:
        df: transformed DataFrame from transform_data()

    Returns:
        tuple: (passed: bool, results: list of ValidationResult)
    """

    logger.info("=" * 50)
    logger.info("VALIDATION: Running data quality checks...")
    logger.info("=" * 50)

    # Define which columns are required
    REQUIRED_COLUMNS = ['iso_code', 'country', 'event_date', 'cases', 'deaths']

    # Run all checks and collect results
    # Each check returns a ValidationResult object
    results = [

        # Check 1: Must have enough rows
        check_not_empty(df, min_rows=100),

        # Check 2: Required columns must exist
        check_required_columns(df, REQUIRED_COLUMNS),

        # Check 3: Critical columns can't have too many nulls
        check_null_rate(df, 'iso_code',    max_null_rate=0.00),  # iso_code: zero nulls allowed
        check_null_rate(df, 'event_date',  max_null_rate=0.00),  # date: zero nulls allowed
        check_null_rate(df, 'cases',       max_null_rate=0.10),  # cases: up to 10% nulls ok

        # Check 4: No negative numbers
        check_no_negatives(df, 'cases'),
        check_no_negatives(df, 'deaths'),

        # Check 5: Dates must be valid
        check_date_range(df, 'event_date'),

        # Check 6: ISO codes must be 3 uppercase letters
        check_iso_code_format(df, 'iso_code'),

        # Check 7: No duplicate (date, country) combinations
        check_unique_combinations(df, ['iso_code', 'event_date']),

        # Check 8: Must have data for many countries
        check_country_coverage(df, min_countries=50),
    ]

    # Print all results
    passed_count = 0
    failed_count = 0

    for result in results:
        if result.passed:
            logger.info(str(result))
            passed_count += 1
        else:
            logger.error(str(result))
            failed_count += 1

    # Summary
    logger.info("=" * 50)
    logger.info(f"VALIDATION SUMMARY: {passed_count} passed, {failed_count} failed")

    all_passed = failed_count == 0

    if all_passed:
        logger.info("VALIDATION: ALL CHECKS PASSED ✓ — safe to load")
    else:
        logger.error(f"VALIDATION: {failed_count} CHECKS FAILED ✗ — pipeline will stop")

    logger.info("=" * 50)

    return all_passed, results


# ============================================================
# HOW TO USE THIS IN YOUR PIPELINE (ingest_who.py):
#
# from etl.validate import validate_health_data
#
# clean_df = transform_data(raw_df)
# passed, results = validate_health_data(clean_df)
#
# if not passed:
#     logger.error("Validation failed — aborting pipeline")
#     sys.exit(1)
#
# load_to_staging(clean_df, engine)  # only runs if validation passed
# ============================================================


# ============================================================
# TEST THE VALIDATOR — Run this file directly to test it
# python etl/validate.py
# ============================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    print("\nTEST 1: Valid data (should pass all checks)")
    print("-" * 40)
    good_df = pd.DataFrame({
        'iso_code':   ['IND', 'USA', 'DEU', 'ARE', 'BRA'] * 30,  # 150 rows
        'country':    ['India', 'United States', 'Germany', 'UAE', 'Brazil'] * 30,
        'event_date': pd.date_range('2024-10-01', periods=150, freq='D'),
        'cases':      [100, 200, 50, 30, 80] * 30,
        'deaths':     [2, 5, 1, 0, 3] * 30,
    })
    passed, _ = validate_health_data(good_df)
    print(f"\nResult: {'PASSED' if passed else 'FAILED'}\n")

    print("\nTEST 2: Bad data (should fail some checks)")
    print("-" * 40)
    bad_df = pd.DataFrame({
        'iso_code':   ['IND', None, 'invalid', 'USA', 'DEU'],  # nulls + invalid
        'country':    ['India', None, 'Unknown', 'USA', 'Germany'],
        'event_date': ['2024-01-15', '2024-01-16', '2025-12-31', '2024-01-17', '2024-01-18'],
        'cases':      [100, -50, 200, 0, 50],  # negative value!
        'deaths':     [2, 1, 5, 0, 1],
    })
    bad_df['event_date'] = pd.to_datetime(bad_df['event_date'])
    passed, _ = validate_health_data(bad_df)
    print(f"\nResult: {'PASSED' if passed else 'FAILED'}\n")
