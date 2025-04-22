"""Collection of general utility functions for the NWEC package."""

import datetime
import subprocess
from datetime import date
from pathlib import Path

import dateutil.parser
import polars as pl


def get_project_root() -> Path:
    """Return the absolute path to the project root directory."""
    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
    ).stdout.strip()
    return Path(root)


def get_previous_quarter(input_date: date) -> tuple[int, int]:
    """Return the previous quarter for the given date."""
    year = input_date.year
    month = input_date.month

    if month in [1, 2, 3]:
        year = year - 1
        quarter = 4
    else:
        quarter = (month - 1) // 3
    return quarter, year


def format_date(date: str, output_format: str = "YYYY-MM-DD", input_format: str | None = None) -> str:
    """Format a date string in a chosen format."""
    if input_format:
        input_date = datetime.datetime.strptime(date, input_format).astimezone(datetime.UTC)
    else:
        input_date = dateutil.parser.parse(date)
    return input_date.strftime(output_format)


def combine_persisted_df(new_df: pl.DataFrame, combined_df_path: Path, export_csv: bool = False) -> pl.DataFrame:
    """Combine a DataFrame with a previously persisted DataFrame, then write the results to the same location.

    DataFrame schemas must match.
    """
    assert combined_df_path.suffix == ".arrow", f"{combined_df_path} must be a .arrow file"
    combined_df_path.parent.mkdir(parents=True, exist_ok=True)
    if combined_df_path.exists():
        combined_df = pl.read_ipc(combined_df_path)
        assert new_df.schema == combined_df.schema, f"{new_df} and {combined_df} do not have the same schema"
        combined_df = pl.concat([new_df, combined_df])
        combined_df = combined_df.unique()
    else:
        combined_df = new_df.unique()

    combined_df.write_ipc(combined_df_path)
    if export_csv:
        combined_df.write_csv(combined_df_path.with_suffix(".csv"))

    return combined_df
