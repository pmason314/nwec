import polars as pl

from nwec.constants import CLEAN_UTILITY_DATA, Utility


def save_processed_arrearages(
    num_arrearages: pl.DataFrame,
    utility: Utility,
    year: int,
    quarter: int,
) -> None:
    """Save the arrearages DataFrame to a CSV file.

    Args:
        num_arrearages (pl.DataFrame): The residential arrearages DataFrame.
        utility (Utility): The utility company for which the arrearages data is being processed.
        year (int): The year of the arrearages data.
        quarter (int): The quarter of the arrearages data.
    """
    output_dir = CLEAN_UTILITY_DATA / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)
    num_arrearages.write_ipc(output_dir / f"{utility.code}_{year}_Q{quarter}_num_arrearages.arrow")
    num_arrearages.write_csv(output_dir / f"{utility.code}_{year}_Q{quarter}_num_arrearages.csv")
