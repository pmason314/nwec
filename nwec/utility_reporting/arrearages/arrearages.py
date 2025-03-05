"""Pipeline functions for calculating arrearages for residential and KLI utility customers."""

import datetime
import itertools
import re
import warnings

import polars as pl

import nwec.utils.excel
from nwec.constants import CLEAN_UTILITY_DATA, Utility
from nwec.utils import format_date


def get_arrearages_df(
    source_df: pl.DataFrame, num_months: int, cols_per_month: int, search_string: int
) -> pl.DataFrame:
    """Locate the the arrearages DataFrame for either residential or KLI customers.

    Args:
        source_df (pl.DataFrame): The raw DataFrame for all arrearage/past-due data
        num_months (int): The number of months described in `source_df`
        cols_per_month (int): The number of data columns per month in `source_df`
        search_string (int): A string or substring to search for in the headers of `source_df`.
            Used to distinguish residential from KLI.

    Returns:
        pl.DataFrame: A narrowed-down arrearages DataFrame.
    """
    _, start_index = nwec.utils.excel.find_cell_by_string(source_df, search_string)
    arrearages = source_df.select(source_df.columns[start_index : start_index + num_months * cols_per_month])

    # Adjust for the calculated DataFrame potentially including the zip code and customer class columns
    # instead of the last two arrearage columns.
    offset = 0
    try:
        nwec.utils.excel.infer_zip_column(arrearages)
        offset += 1
    except ValueError:
        pass
    try:
        infer_customer_class_column(arrearages)
        offset += 1
    except ValueError:
        pass
    return source_df.select(
        source_df.columns[start_index + offset : start_index + num_months * cols_per_month + offset]
    )


def combine_arrearage_year_vintage_cols(
    arrearages_df: pl.DataFrame, num_months: int, cols_per_month: int, source_date_format: str | None = None
) -> pl.DataFrame:
    """Clean and rename the columns of an arrearages DataFrame to reflect the month and year of the data.

    Args:
        arrearages_df (pl.DataFrame): The narrowed DataFrame for either residential or KLI arrearages.
        num_months (int): The number of months described in `arrearages_df`.
        cols_per_month (int): The number of data columns per month in `arrearages_df`.
        source_date_format (str): The format of the date in the source data, e.g. "%Y-%m-%d %H:%M:%S".

    Returns:
        pl.DataFrame: The cleaned and renamed arrearages DataFrame.
    """
    months = arrearages_df.slice(0, 1).to_dicts()[0]
    months = [v for v in months.values() if v is not None]
    if len(months) < num_months:
        months = extrapolate_missing_months(months, num_months, source_date_format)
    assert len(months) == num_months, f"Expected {num_months} months, found {len(months)}."

    vintages = arrearages_df.slice(1, 2).to_dicts()[0]
    vintages = list(itertools.islice(vintages.values(), 0, cols_per_month))
    assert len(vintages) == cols_per_month, f"Expected {cols_per_month} vintages per month, found {len(vintages)}."

    new_date_columns = []

    for month in months:
        for vintage in vintages:
            formatted_month = format_date(month, "%Y %m", source_date_format)
            new_date_columns.append(f"{formatted_month} {vintage}")

    new_date_columns = dict(zip(arrearages_df.columns, new_date_columns, strict=True))
    return arrearages_df.rename(new_date_columns).tail(-2)


def extrapolate_missing_months(months: list[str], num_months: int, source_date_format: str) -> list[str]:
    """Extrapolate missing initial months in the arrearages DataFrame.  Assumes that the initial month(s) are missing."""
    date_months = [datetime.datetime.strptime(month, source_date_format).astimezone(datetime.UTC) for month in months]
    delta = date_months[1] - date_months[0]
    while len(date_months) < num_months:
        date_months.insert(0, date_months[0] - delta)
    return [month.strftime(source_date_format) for month in date_months]


def normalize_vintage_cols(arrearages_df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the names of the vintage columns."""
    vintage_regex = r"(\d{4} \d{2}).*(\d{2})"

    for col in arrearages_df.columns:
        match = re.search(vintage_regex, col)
        if match:
            vintage = match.group(2)
            if vintage in ("30", "31"):
                vintage = 30
            elif vintage in ("60", "61"):
                vintage = 60
            elif vintage in ("90", "91"):
                vintage = 90
            else:
                raise ValueError(f"Unexpected vintage found: {vintage}")
            new_vintage_col_name = f"{match.group(1)} {vintage}"
            if new_vintage_col_name in arrearages_df.columns:
                arrearages_df = arrearages_df.drop(col)
            else:
                arrearages_df = arrearages_df.rename({col: f"{match.group(1)} {vintage}"})
        else:
            arrearages_df = arrearages_df.drop(col)
    return arrearages_df


def add_zip_and_customer_class_cols(spreadsheet_df: pl.DataFrame, arrearages_df: pl.DataFrame) -> pl.DataFrame:
    """Add the ZIP code and customer class columns to the arrearages DataFrame."""
    zip_column = spreadsheet_df.select(pl.nth(nwec.utils.excel.infer_zip_column(spreadsheet_df)))
    zip_row = nwec.utils.excel.find_cell_by_string(zip_column, "zip")
    zip_column = zip_column.rename({zip_column.columns[0]: "Zip Code"}).tail(-(zip_row[0] + 1))
    zip_column = zip_column.with_columns(pl.col("Zip Code").str.strip_chars())
    customer_class_column = spreadsheet_df.select(pl.nth(infer_customer_class_column(spreadsheet_df)))
    customer_class_row = nwec.utils.excel.find_cell_by_string(customer_class_column, "class")
    customer_class_column = customer_class_column.rename({customer_class_column.columns[0]: "Customer Class"}).tail(
        -(customer_class_row[0] + 1)
    )
    return pl.concat([zip_column, customer_class_column, arrearages_df], how="horizontal")


def normalize_arrearage_cols(arrearages_df: pl.DataFrame, arrearage_type: str, utility: Utility) -> pl.DataFrame:
    """Normalize the columns of an arrearages DataFrame to have consistent names regardless of data source.

    Args:
        arrearages_df (pl.DataFrame): A DataFrame for either residential or KLI arrearages.
        arrearage_type (str): The type of arrearages data being processed, e.g. "Residential" or "KLI".
        utility (Utility): The utility company for which the arrearages data is being processed.

    Returns:
        pl.DataFrame: The normalized arrearages DataFrame.
    """
    assert arrearage_type in ("Residential", "KLI")
    arrearages_df = arrearages_df.filter(~pl.all_horizontal(pl.all().is_null()))
    arrearages_df = arrearages_df.filter(pl.col("Zip Code").is_not_null())
    arrearages_df = arrearages_df.filter(pl.col("Zip Code").str.len_chars() > 0)

    # Filter out non-residential classes
    arrearages_df = arrearages_df.filter(pl.col("Customer Class").str.contains(r"(?i)res"))
    arrearages_df = arrearages_df.drop("Customer Class")
    arrearages_df = arrearages_df.unpivot(index="Zip Code")
    arrearages_df = arrearages_df.with_columns(pl.col("variable").str.split_exact(" ", 2)).unnest("variable")

    arrearages_df = arrearages_df.rename(
        {"field_0": "Year", "field_1": "Month", "field_2": "Vintage", "value": "Amount"}
    )
    arrearages_df = arrearages_df.with_columns(pl.lit(utility.full_name).alias("Utility"))
    arrearages_df = arrearages_df.with_columns(pl.lit(arrearage_type).alias("Customer Class"))
    arrearages_df = arrearages_df.with_columns(pl.col("Amount").fill_null(0))

    return arrearages_df.cast({"Year": pl.Int32, "Month": pl.Int32, "Vintage": pl.Int32, "Amount": pl.Float64})


def infer_customer_class_column(df: pl.DataFrame, num_rows: int = 25, threshold: int = 5) -> int:
    """Infer the column index of the ZIP code column in a DataFrame."""
    customer_class_regex = r"^com|res|ind|gov|lrg|sm"

    # Keep track of the number of rows in each column that match the customer class regex
    customer_class_counts = []
    for x in range(df.width):
        count = 0
        for y in range(num_rows):
            if df.item(y, x) is None:
                continue
            if re.match(customer_class_regex, str(df.item(y, x)).strip().lower()):
                count += 1
        customer_class_counts.append(count)

    # Check if multiple columns have at least `threshold` rows that match the customer class regex
    customer_class_columns = [i for i, count in enumerate(customer_class_counts) if count >= threshold]
    if len(customer_class_columns) == 1:
        return customer_class_columns[0]
    if len(customer_class_columns) > 1:
        warnings.warn(
            f"Multiple columns have at least {threshold} rows that match the customer class pattern; using the first.",
            stacklevel=2,
        )
        return customer_class_columns[0]
    raise ValueError(f"No columns have at least {threshold} rows that match the customer class pattern.")


def infer_date_row(df: pl.DataFrame, date_format: str, num_rows: int = 25, threshold: int = 2) -> int:
    """Infer the row index of the date column in a DataFrame."""
    # Keep track of the number of rows in each column that match the date regex
    date_counts = []
    for y in range(num_rows):
        count = 0
        for x in range(df.width):
            if df.item(y, x) is None:
                continue
            try:
                datetime.datetime.strptime(df.item(y, x), date_format).astimezone(datetime.UTC)
                count += 1

            except ValueError:
                continue
        date_counts.append(count)

    # Check if multiple rows have at least 5 columns that match the date regex
    date_rows = [i for i, count in enumerate(date_counts) if count >= threshold]
    if len(date_rows) == 1:
        return date_rows[0]
    if len(date_rows) > 1:
        warnings.warn(
            f"Multiple rows have at least {threshold} columns that match the date pattern; using the first.",
            stacklevel=2,
        )
        return date_rows[0]
    raise ValueError(f"No rows have at least {threshold} columns that match the date pattern.")


def save_processed_arrearages(arrearages: pl.DataFrame, kli_arrearages: pl.DataFrame) -> None:
    """Save the arrearages DataFrame to a CSV file.

    Args:
        arrearages (pl.DataFrame): The residential arrearages DataFrame.
        kli_arrearages (pl.DataFrame): The KLI arrearages DataFrame.
    """
    CLEAN_UTILITY_DATA.mkdir(parents=True, exist_ok=True)
    arrearage_path = CLEAN_UTILITY_DATA / "arrearage_amounts.arrow"

    if arrearage_path.exists():
        combined_arrearages = pl.read_ipc(arrearage_path)
        combined_arrearages = pl.concat([combined_arrearages, arrearages, kli_arrearages])
        combined_arrearages = combined_arrearages.unique()
    else:
        combined_arrearages = pl.concat([arrearages, kli_arrearages])
    combined_arrearages.write_ipc(CLEAN_UTILITY_DATA / "arrearage_amounts.arrow")
    combined_arrearages.write_csv(CLEAN_UTILITY_DATA / "arrearage_amounts.csv")
