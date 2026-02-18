"""Common functionality for processing assistance pipelines."""

from pathlib import Path

import polars as pl

import nwec.utils.excel
from nwec.constants import MONTHS, PROCESSED_UTILITY_DATA, PROJECT_ROOT
from nwec.utility_reporting.processing.standard_pipeline import (
    MASTER_SPREADSHEET,
    Pipeline,
    load_pipelines,
)
from nwec.utils.cleaning import clean_utility_data, validate_data


def run_full_pipeline(pipeline: Pipeline) -> None:
    """Run the full assistance processing pipeline for a given configuration.

    Args:
        pipeline: Pipeline configuration
    """
    sheet_index = nwec.utils.excel.get_sheet_index_from_name(MASTER_SPREADSHEET, pipeline.sheet_name)
    df = pl.read_excel(MASTER_SPREADSHEET, sheet_id=sheet_index, has_header=False)

    # Get rid of the first row and promote the second to be the DataFrame header
    headers = df.slice(1, 1).row(0)
    df = df.slice(2).rename(dict(zip(df.columns, [str(col) for col in headers], strict=True)))

    index_cols = [col for col in df.columns if col not in MONTHS]

    # Unpivot the month columns
    df = df.unpivot(on=MONTHS, index=index_cols, variable_name="Month", value_name=pipeline.core_value_column_name)
    # Convert month names to integers (1-12)
    df = df.with_columns(pl.col("Month").str.to_datetime("%B").dt.month().alias("Month"))

    df = clean_utility_data(df, value_column_name=pipeline.core_value_column_name)
    validate_data(df, value_column_name=pipeline.core_value_column_name, sheet_name=pipeline.sheet_name)

    # Save the processed data
    PROCESSED_UTILITY_DATA.mkdir(parents=True, exist_ok=True)
    processed_path = PROCESSED_UTILITY_DATA / f"{pipeline.processed_file_name}.arrow"

    if processed_path.exists():
        combined_assistance = pl.read_ipc(processed_path)
        combined_assistance = pl.concat([combined_assistance, df])
        combined_assistance = combined_assistance.unique()
    else:
        combined_assistance = df

    # Sort by all columns to ensure consistent output order
    combined_assistance = combined_assistance.sort(combined_assistance.columns)

    processed_path.unlink(missing_ok=True)
    combined_assistance.write_ipc(processed_path)
    combined_assistance.write_csv(processed_path.with_suffix(".csv"))
    print(
        f"Saved '{pipeline.sheet_name}' to {processed_path.relative_to(PROJECT_ROOT)} and "
        f"{processed_path.with_suffix('.csv').relative_to(PROJECT_ROOT)}"
    )
