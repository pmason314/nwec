"""Pipeline functions for calculating arrearages for residential and KLI utility customers."""

import datetime

import polars as pl

import nwec.utils.excel


def normalize_zip_class_cols(source_df: pl.DataFrame, arrearages_df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the zip code and customer class columns in any arrearage DataFrame.

    Args:
        source_df (pl.DataFrame): The raw DataFrame for all arrearage/past-due data.
        arrearages_df (pl.DataFrame): The narrowed DataFrame for either residential or KLI arrearages.

    Returns:
        pl.DataFrame: The arrearages DataFrame with the zip code in the first column and customer class in the second.
    """
    class_index, zip_index = (-1, -1), (-1, -1)
    normalized_df = arrearages_df
    class_permutations = [
        (arrearages_df, "Customer Class"),
        (arrearages_df, "Class"),
        (source_df, "Customer Class"),
        (source_df, "Class"),
    ]
    for df, search_string in class_permutations:
        try:
            class_index = nwec.utils.excel.find_unpromoted_header(df, search_string, exact_match=True)
            if df.equals(source_df):
                normalized_df = pl.concat(
                    [
                        source_df.select(pl.nth(class_index[1])),
                        normalized_df,
                    ],
                    how="horizontal",
                )
            break
        except ValueError:
            class_index = None

    zip_permutations = [
        (arrearages_df, "Zip Code"),
        (arrearages_df, "Zip"),
        (source_df, "Zip Code"),
        (source_df, "Zip"),
    ]
    for df, search_string in zip_permutations:
        try:
            zip_index = nwec.utils.excel.find_unpromoted_header(df, search_string, exact_match=True)
            if df.equals(source_df):
                normalized_df = pl.concat(
                    [
                        source_df.select(pl.nth(zip_index[1])),
                        normalized_df,
                    ],
                    how="horizontal",
                )
            break
        except ValueError:
            zip_index = None
    normalized_df = normalized_df.rename(
        {normalized_df.columns[zip_index[1]]: "Zip Code", normalized_df.columns[class_index[1]]: "Customer Class"}
    )

    assert zip_index[0] == class_index[0], "Customer class and zip code are not in the same row."
    return normalized_df


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
    _, start_index = nwec.utils.excel.find_unpromoted_header(source_df, search_string)
    arrearages = source_df.select(source_df.columns[start_index : start_index + num_months * cols_per_month])
    return normalize_zip_class_cols(source_df, arrearages)


def rename_arrearage_columns(
    arrearages_df: pl.DataFrame, cols_per_month: int, date_to_zip_offset: int, source_date_format: str
) -> pl.DataFrame:
    """Clean and rename the columns of an arrearages DataFrame to reflect the month and year of the data.

    Args:
        arrearages_df (pl.DataFrame): The narrowed DataFrame for either residential or KLI arrearages.
        cols_per_month (int): The number of data columns per month in `arrearages_df`.
        date_to_zip_offset (int): The number of rows between the date row and the zip code row in `arrearages_df`.
        source_date_format (str): The format of the date in the source data, e.g. "%Y-%m-%d %H:%M:%S".

    Returns:
        pl.DataFrame: The cleaned and renamed arrearages DataFrame.
    """
    try:
        zip_index = nwec.utils.excel.find_unpromoted_header(arrearages_df, "Zip Code")
    except ValueError:
        zip_index = nwec.utils.excel.find_unpromoted_header(arrearages_df, "Zip")
    date_row = zip_index[0] - date_to_zip_offset

    new_columns = arrearages_df.select(arrearages_df.columns[2:]).slice(date_row, 1).to_dicts()[0]
    vintage_cols = arrearages_df.select(arrearages_df.columns[2:]).slice(date_row + 1, 1).to_dicts()[0]
    months = list({k: v for k, v in new_columns.items() if v is not None}.values())

    if len(vintage_cols) > cols_per_month * len(months) + 2:  # Try to detect if a month cell is missing
        extra_columns = arrearages_df.select(arrearages_df.columns).slice(date_row, 1).to_dicts()[0]
        months = list({k: v for k, v in extra_columns.items() if v is not None}.values())
    assert len(vintage_cols) <= cols_per_month * len(months) + 2, "Could not find missing month cell"

    for counter, col in enumerate(vintage_cols):
        current_month = months[counter // cols_per_month]
        date = datetime.datetime.strptime(current_month, source_date_format).astimezone(datetime.UTC)
        new_columns[col] = date.strftime("%B %Y")
        new_columns[col] = new_columns[col] + " " + vintage_cols[col]
    new_columns = new_columns | {"Zip Code": "Zip Code", "Customer Class": "Customer Class"}

    arrearages_df = arrearages_df.rename(new_columns)
    arrearages_df = arrearages_df.select(arrearages_df.columns[: len(new_columns)])
    arrearages_df = arrearages_df.filter(~pl.all_horizontal(pl.all().is_null()))
    arrearages_df = arrearages_df.with_columns(
        [pl.col(col).cast(pl.Float64, strict=False) for col in arrearages_df.columns[2:]]
    )
    # Filter out non-residential classes
    arrearages_df = arrearages_df.filter(pl.col("Customer Class").str.contains(r"(?i)res"))

    # Drop extraneous zip code and customer class columns
    return pl.concat(
        [arrearages_df.select("Zip Code"), arrearages_df.drop(pl.selectors.matches(r"(?i)zip|customer class"))],
        how="horizontal",
    )
