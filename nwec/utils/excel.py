"""Collection of utilities for working with Excel spreadsheets."""

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


def find_unpromoted_header(
    df: pl.DataFrame, search_string: str, exact_match: bool = False, search_x: int = 0, search_y: int = 15
) -> tuple[int, int]:
    """Find the indices of a column name in a DataFrame based on a search string.

    Used to find a cell that is the name of a column in the source spreadsheet, but is not the column name in the
    DataFrame.
    """
    search_string = search_string.lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ").strip()
    if search_x == 0:
        search_x = df.width
    for x in range(search_x):
        for y in range(search_y):
            if df.item(y, x) is None:
                continue
            clean_target_string = (
                str(df.item(y, x)).lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ").strip()
            )
            if (
                exact_match
                and search_string == clean_target_string
                or (
                    not exact_match and search_string in clean_target_string
                    # and abs(len(search_string) - len(clean_target_string)) < 5
                )
            ):
                return y, x
    raise ValueError(f"'{search_string}' not found in the first {search_y} rows and {search_x} columns of DataFrame.")
