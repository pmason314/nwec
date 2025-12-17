"""Utility data cleaning functions."""

import polars as pl

from nwec.constants import Utility


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


def _validate_value_column(df: pl.DataFrame, value_column_name: str) -> dict[str, list[str]]:
    """Validate the value column for negative numbers."""
    errors = {}
    negative_values = df.filter(pl.col(value_column_name) < 0)
    if len(negative_values) > 0:
        count = len(negative_values)
        errors[value_column_name] = [f"Found {count} negative values"]
    return errors


def validate_data(df: pl.DataFrame, value_column_name: str) -> dict[str, list[str]]:
    """Validate utility data and return any issues found.

    Args:
        df: Input DataFrame to validate
        value_column_name: Name of the column containing numeric values to validate

    Returns:
        Dictionary mapping validation check names to lists of error messages
    """
    errors = {}

    if "Utility" in df.columns:
        errors.update(_validate_utility_column(df))

    if "Zip Code" in df.columns:
        errors.update(_validate_zip_code_column(df))

    if "Year" in df.columns:
        errors.update(_validate_year_column(df))

    if "Month" in df.columns:
        errors.update(_validate_month_column(df))

    if value_column_name in df.columns:
        errors.update(_validate_value_column(df, value_column_name))

    if errors:
        error_message = "Data validation failed:\n"
        for field, messages in errors.items():
            for message in messages:
                error_message += f"  - {field}: {message}\n"
        raise ValueError(error_message)

    return errors
