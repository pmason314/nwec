"""Pipeline functions for calculating arrearages for residential and KLI utility customers."""

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
