"""Collection of utilities for working with Excel spreadsheets."""

import re
import warnings
from pathlib import Path

import openpyxl
import polars as pl


def get_sheet_index_from_name(spreadsheet_path: Path, sheet_name: str) -> int:
    """Return the index of the first Excel sheet containing the given string."""
    workbook = openpyxl.load_workbook(spreadsheet_path, read_only=True)
    for index, sheet in enumerate(workbook.sheetnames):
        if sheet_name.lower() in sheet.lower():
            return index + 1
    raise NameError(f"Sheet '{sheet_name}' not found in '{spreadsheet_path}'.")


def find_cell_by_string(
    df: pl.DataFrame, search_string: str, exact_match: bool = False, num_search_rows: int = 15
) -> tuple[int, int]:
    """Find the indices of a cell in a DataFrame based on a search string.

    Used to find a cell that is the name of a column in the source spreadsheet, but is not the column name in the
    DataFrame.
    """
    search_string = search_string.lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ").strip()
    for x in range(df.width):
        for y in range(num_search_rows):
            if df.item(y, x) is None:
                continue
            clean_target_string = (
                str(df.item(y, x)).lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ").strip()
            )
            if (exact_match and search_string == clean_target_string) or (
                not exact_match and search_string in clean_target_string
            ):
                return y, x
    raise ValueError(f"'{search_string}' not found in the first {num_search_rows} rows of DataFrame.")


def infer_zip_column(df: pl.DataFrame, num_rows: int = 25, threshold: int = 5, start_col: int = 0) -> int:
    """Infer the column index of the ZIP code column in a DataFrame."""
    zip_regex = r"^\d{5}(-\d{4})?$"

    # Keep track of the number of rows in each column that match the ZIP code regex
    zip_counts = []
    for x in range(start_col, df.width):
        count = 0
        for y in range(num_rows):
            if df.item(y, x) is None:
                continue
            if re.match(zip_regex, str(df.item(y, x)).strip().lower()):
                count += 1
        zip_counts.append(count)

    # Check if multiple columns have at least 5 rows that match the ZIP code regex
    zip_columns = [i for i, count in enumerate(zip_counts, start=start_col) if count >= threshold]
    print(zip_columns)
    if len(zip_columns) == 1:
        return zip_columns[0]
    if len(zip_columns) > 1:
        warnings.warn(
            f"Multiple columns have at least {threshold} rows that match the ZIP code pattern; using the first.",
            stacklevel=2,
        )
        return zip_columns[0]
    raise ValueError(f"No columns have at least {threshold} rows that match the ZIP code pattern.")
