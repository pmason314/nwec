"""Collection of general utility functions for the NWEC package."""

import subprocess
from datetime import date
from pathlib import Path


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
