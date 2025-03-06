import polars as pl

from nwec.constants import CLEAN_UTILITY_DATA, Utility
from nwec.utils import format_date


def format_arrearage_count_dates(arrearages_df, source_date_format):
    months = arrearages_df.slice(0, 1).to_dicts()[0].values()
    new_date_columns = []
    for month in months:
        formatted_month = format_date(month, "%Y %m", source_date_format)
        new_date_columns.append(f"{formatted_month}")

    new_date_columns = dict(zip(arrearages_df.columns, new_date_columns, strict=True))
    return arrearages_df.rename(new_date_columns).tail(-1)


def normalize_arrearage_count_cols(arrearages_df, utility):
    arrearages_df = arrearages_df.filter(~pl.all_horizontal(pl.all().is_null()))
    arrearages_df = arrearages_df.filter(pl.col("Zip Code").is_not_null())
    arrearages_df = arrearages_df.filter(pl.col("Zip Code").str.len_chars() > 0)

    # Filter out non-residential classes
    arrearages_df = arrearages_df.filter(pl.col("Customer Class").str.contains(r"(?i)res"))
    arrearages_df = arrearages_df.drop("Customer Class")
    arrearages_df = arrearages_df.unpivot(index="Zip Code")
    arrearages_df = arrearages_df.with_columns(pl.col("variable").str.split_exact(" ", 1)).unnest("variable")

    arrearages_df = arrearages_df.rename({"field_0": "Year", "field_1": "Month", "value": "Count"})
    arrearages_df = arrearages_df.with_columns(pl.lit(utility.full_name).alias("Utility"))
    arrearages_df = arrearages_df.with_columns(pl.lit("Residential").alias("Customer Class"))
    arrearages_df = arrearages_df.with_columns(pl.col("Count").fill_null(0))

    return arrearages_df.cast({"Year": pl.Int32, "Month": pl.Int32, "Count": pl.Int32})


def save_processed_arrearage_counts(arrearages: pl.DataFrame) -> None:
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
