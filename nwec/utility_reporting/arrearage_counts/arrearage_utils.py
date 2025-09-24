"""Pipeline functions for calculating arrearage counts for residential customers."""

import polars as pl

from nwec.constants import CLEAN_UTILITY_DATA


def save_arrearage_counts(arrearages: pl.DataFrame) -> None:
    """Save the arrearage counts DataFrame to a CSV file.

    Args:
        arrearages (pl.DataFrame): The residential arrearage counts DataFrame.
    """
    CLEAN_UTILITY_DATA.mkdir(parents=True, exist_ok=True)
    arrearage_path = CLEAN_UTILITY_DATA / "arrearage_counts.arrow"

    if arrearage_path.exists():
        combined_arrearage_counts = pl.read_ipc(arrearage_path)
        combined_arrearage_counts = pl.concat([combined_arrearage_counts, arrearages])
        combined_arrearage_counts = combined_arrearage_counts.unique()
    else:
        combined_arrearage_counts = arrearages

    combined_arrearage_counts.write_ipc(CLEAN_UTILITY_DATA / "arrearage_counts.arrow")
    combined_arrearage_counts.write_csv(CLEAN_UTILITY_DATA / "arrearage_counts.csv")
