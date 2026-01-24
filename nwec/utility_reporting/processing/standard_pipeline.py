"""Common functionality for standard processing pipelines."""

from pathlib import Path

import polars as pl
import yaml
from pydantic import BaseModel, Field

import nwec.utils.excel
from nwec.constants import MONTHS, PROCESSED_UTILITY_DATA, PROJECT_ROOT, RAW_UTILITY_DATA
from nwec.utils.cleaning import clean_utility_data, validate_data

MASTER_SPREADSHEET = RAW_UTILITY_DATA / "IOU 200281 Data.xlsx"


class Pipeline(BaseModel):
    """Pydantic model to hold standard pipeline configuration."""

    sheet_name: str = Field(..., description="Name of the Excel sheet to process")
    core_value_column_name: str = Field(..., description="Name of the main value column")
    processed_file_name: str = Field(..., description="Output filename (without extension)")


class PipelineConfig(BaseModel):
    """Container for multiple pipeline configurations."""

    pipelines: list[Pipeline]


def load_pipelines(config_path: Path | None = None) -> list[Pipeline]:
    """Load and validate pipeline configurations from YAML file.

    Args:
        config_path: Path to YAML config file. Defaults to pipelines.yaml in same directory.

    Returns:
        List of validated Pipeline objects
    """
    if config_path is None:
        config_path = Path(__file__).parent / "pipelines.yaml"

    with Path(config_path).open() as f:
        config_data = yaml.safe_load(f)

    config = PipelineConfig(**config_data)
    return config.pipelines


def run_full_pipeline(pipeline: Pipeline) -> None:
    """Run the full standard processing pipeline for a given configuration.

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

    # Filter for residential customers and clean up the "Arrearage Count" column
    df = df.filter(pl.col("Customer Class").str.contains(r"(?i)res")).drop("Customer Class")
    df = clean_utility_data(df, value_column_name=pipeline.core_value_column_name)
    validate_data(df, value_column_name=pipeline.core_value_column_name, sheet_name=pipeline.sheet_name)

    # Save the processed data
    PROCESSED_UTILITY_DATA.mkdir(parents=True, exist_ok=True)
    processed_path = PROCESSED_UTILITY_DATA / f"{pipeline.processed_file_name}.arrow"

    if processed_path.exists():
        combined_payment_agreements = pl.read_ipc(processed_path)
        combined_payment_agreements = pl.concat([combined_payment_agreements, df])
        combined_payment_agreements = combined_payment_agreements.unique()
    else:
        combined_payment_agreements = df

    processed_path.unlink(missing_ok=True)
    combined_payment_agreements.write_ipc(processed_path)
    combined_payment_agreements.write_csv(processed_path.with_suffix(".csv"))
    print(
        f"Saved '{pipeline.sheet_name}' to {processed_path.relative_to(PROJECT_ROOT)} and "
        f"{processed_path.with_suffix('.csv').relative_to(PROJECT_ROOT)}"
    )
