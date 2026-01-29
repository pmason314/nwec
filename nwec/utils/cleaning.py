"""Utility data cleaning functions."""

import polars as pl

from nwec.constants import Utility


def manual_cleaning(df: pl.DataFrame) -> pl.DataFrame:
    """Apply manual cleaning steps to the utility data DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        Manually cleaned DataFrame
    """
    # Remove rows containing "(blank)" or "(blanks)" in any column
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.filter(~pl.col(col).str.to_lowercase().is_in(["(blank)", "(blanks)"]))

    return df


def clean_utility_data(df: pl.DataFrame, value_column_name: str) -> pl.DataFrame:
    """Clean utility data by removing empty/null values and converting columns to appropriate types.

    Args:
        df: Input DataFrame
        value_column_name: Name of the column containing numeric values to clean

    Returns:
        Cleaned DataFrame with proper data types
    """
    # Strip whitespace and replace empty strings with nulls across all string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().alias(col))
            df = df.with_columns(pl.when(pl.col(col) == "").then(None).otherwise(pl.col(col)).alias(col))
    df = df.drop_nulls()

    if "Year" in df.columns:
        df = df.with_columns(pl.col("Year").cast(pl.Int64))

    # Convert month names to integers (1-12)
    if "Month" in df.columns and df["Month"].dtype == pl.Utf8:
        df = df.with_columns(pl.col("Month").str.to_datetime("%B").dt.month().alias("Month"))

    df = df.with_columns(pl.col(value_column_name).cast(pl.Float64))

    is_all_integers = (df[value_column_name] % 1 == 0).all()

    if is_all_integers:
        df = df.with_columns(pl.col(value_column_name).cast(pl.Int64))

    # Reorder columns: Utility, Year, Month should be first if they exist
    columns_to_reorder = ["Utility", "Year", "Month"]
    first_cols = [col for col in columns_to_reorder if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in first_cols]
    if first_cols:
        df = df.select(first_cols + remaining_cols)

    df = manual_cleaning(df)

    return df


def _validate_utility_column(df: pl.DataFrame) -> dict[str, list[str]]:
    """Validate the Utility column."""
    errors = {}
    valid_utilities = {util.code for util in Utility}
    invalid_utilities = df.filter(~pl.col("Utility").is_in(valid_utilities))
    if len(invalid_utilities) > 0:
        unique_invalid = invalid_utilities["Utility"].unique().to_list()
        errors["Utility"] = [f"Found invalid utility values: {unique_invalid}. Valid values are: {valid_utilities}"]
    return errors


def _validate_zip_code_column(df: pl.DataFrame) -> dict[str, list[str]]:
    """Validate the Zip Code column."""
    errors = {}
    invalid_zips = df.filter(~pl.col("Zip Code").cast(pl.Utf8).str.contains(r"^\d{5}(-\d{4})?$"))
    if len(invalid_zips) > 0:
        unique_invalid = invalid_zips["Zip Code"].unique().to_list()
        errors["Zip Code"] = [f"Found invalid zip codes: {unique_invalid[:10]}"]
    return errors


def _validate_year_column(df: pl.DataFrame) -> dict[str, list[str]]:
    """Validate the Year column."""
    errors = {}
    invalid_years = df.filter(pl.col("Year") < 2020)
    if len(invalid_years) > 0:
        unique_invalid = invalid_years["Year"].unique().sort().to_list()
        errors["Year"] = [f"Found years <= 2020: {unique_invalid}"]
    return errors


def _validate_month_column(df: pl.DataFrame) -> dict[str, list[str]]:
    """Validate the Month column."""
    errors = {}
    invalid_months = df.filter((pl.col("Month") < 1) | (pl.col("Month") > 12))
    if len(invalid_months) > 0:
        unique_invalid = invalid_months["Month"].unique().sort().to_list()
        errors["Month"] = [f"Found months outside 1-12 range: {unique_invalid}"]
    return errors


def _check_negative_values(df: pl.DataFrame, value_column_name: str) -> dict[str, list[str]]:
    """Check the value column for negative numbers (warning only)."""
    warnings = {}
    negative_values = df.filter(pl.col(value_column_name) < 0)
    if len(negative_values) > 0:
        count = len(negative_values)
        warnings[value_column_name] = [f"Found {count} negative values"]
    return warnings


def _validate_duplicates(df: pl.DataFrame) -> dict[str, list[str]]:
    """Validate that there are no duplicate rows based on key columns.

    Checks for duplicates based on Utility, Year, Month, Customer Class (if present),
    Zip Code (if present), and Vintage (if present). This catches cases where multiple
    rows exist for the same key combination, which could happen if data is split across
    quarters or reporting periods.
    """
    errors = {}

    # Build list of key columns that exist in the dataframe
    key_columns: list[str] = [
        col for col in ["Utility", "Year", "Month", "Customer Class", "Zip Code", "Vintage"] if col in df.columns
    ]

    if not key_columns:
        return errors

    # Check for duplicates based on key columns
    duplicate_check = df.select(key_columns).group_by(key_columns).agg(pl.len().alias("count"))
    duplicates = duplicate_check.filter(pl.col("count") > 1)

    if len(duplicates) > 0:
        count = len(duplicates)
        sample_duplicates = duplicates.head(5).to_dicts()
        errors["Duplicates"] = [
            f"Found {count} duplicate key combinations. "
            f"Sample duplicates: {sample_duplicates}. "
            f"This may indicate overlapping data from different reporting periods."
        ]

    return errors


def _handle_validation_results(
    errors: dict[str, list[str]], warnings: dict[str, list[str]], sheet_name: str | None = None
) -> None:
    """Print warnings and raise errors if validation failed."""
    sheet_info = f" (Sheet: {sheet_name})" if sheet_name else ""

    # Print warnings if any
    if warnings:
        print(f"Data validation warnings{sheet_info}:")
        for field, messages in warnings.items():
            for message in messages:
                print(f"  - {field}: {message}")

    # Raise error if there are actual errors
    if errors:
        error_message = f"Data validation failed{sheet_info}:\n"
        for field, messages in errors.items():
            for message in messages:
                error_message += f"  - {field}: {message}\n"
        raise ValueError(error_message)

    sheet_info_msg = f" for sheet '{sheet_name}'" if sheet_name else ""
    print(f"Data validation passed with no errors{sheet_info_msg}.")


def validate_data(df: pl.DataFrame, value_column_name: str, sheet_name: str | None = None) -> None:
    """Validate utility data and return any issues found.

    Args:
        df: Input DataFrame to validate
        value_column_name: Name of the column containing numeric values to validate
        sheet_name: Optional name of the sheet being validated (for error reporting)
    """
    errors = {}
    warnings = {}

    if "Utility" in df.columns:
        errors.update(_validate_utility_column(df))

    if "Zip Code" in df.columns:
        errors.update(_validate_zip_code_column(df))

    if "Year" in df.columns:
        errors.update(_validate_year_column(df))

    if "Month" in df.columns:
        errors.update(_validate_month_column(df))

    if value_column_name in df.columns:
        warnings.update(_check_negative_values(df, value_column_name))

    # Check for duplicates based on key columns
    errors.update(_validate_duplicates(df))

    _handle_validation_results(errors, warnings, sheet_name)
