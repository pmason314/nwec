"""Avista Pipeline."""

import polars as pl

import nwec.utility_reporting.arrearages
import nwec.utils
import nwec.utils.excel
from nwec.constants import CLEAN_UTILITY_DATA, RAW_UTILITY_DATA, Utility

NUM_MONTHS = 12
COLS_PER_MONTH = 4
SHEET_SEARCH_STRING = "past due balances"
ARREARAGE_SEARCH_STRING = "past-due balances by customer class"
KLI_SEARCH_STRING = "past-due balances for known low-income household"
source_date_format = "%Y-%m-%d %H:%M:%S"


# Avista includes room for 12 months every time
# Need to test whether Q4 files duplicate Q1-Q3 in the resulting DataFrame
def avista_arrearages(year, quarter):
    print(f"Processing Avista arrearages for {year} Q{quarter}")
    num_months = 9 if year == 2020 and quarter == 4 else 12

    spreadsheet = RAW_UTILITY_DATA / str(year) / f"{Utility.AVISTA.code}_{year}_Q{quarter}.xlsx"
    sheet_index = nwec.utils.excel.get_sheet_index_from_name(spreadsheet, SHEET_SEARCH_STRING)

    df = pl.read_excel(spreadsheet, sheet_id=sheet_index, has_header=False)
    arrearages = nwec.utility_reporting.arrearages.get_arrearages_df(
        df, num_months, COLS_PER_MONTH, ARREARAGE_SEARCH_STRING
    )

    date_row = nwec.utility_reporting.arrearages.infer_date_row(arrearages, source_date_format)
    arrearages = arrearages.tail(-date_row)  # remove rows before the date row
    arrearages = nwec.utility_reporting.arrearages.combine_arrearage_year_vintage_cols(
        arrearages, num_months, COLS_PER_MONTH
    )

    arrearages = nwec.utility_reporting.arrearages.normalize_vintage_cols(arrearages)
    arrearages = nwec.utility_reporting.arrearages.add_zip_and_customer_class_cols(df, arrearages)
    arrearages = nwec.utility_reporting.arrearages.normalize_arrearage_cols(arrearages, "Residential", Utility.AVISTA)
    arrearages = arrearages.filter(pl.col("Amount") > 0)

    assert (
        arrearages.select(pl.struct(["Zip Code", "Year", "Month", "Vintage"])).is_unique().all()
    ), "Duplicate rows detected"

    nwec.utils.combine_persisted_df(arrearages, CLEAN_UTILITY_DATA / "arrearage_amounts.arrow", export_csv=True)
    return arrearages


def avista_kli_arrearages(year, quarter):
    print(f"Processing Avista KLI arrearages for {year} Q{quarter}")

    num_months = 9 if year == 2020 and quarter == 4 else 12

    spreadsheet = RAW_UTILITY_DATA / str(year) / f"{Utility.AVISTA.code}_{year}_Q{quarter}.xlsx"
    sheet_index = nwec.utils.excel.get_sheet_index_from_name(spreadsheet, SHEET_SEARCH_STRING)

    df = pl.read_excel(spreadsheet, sheet_id=sheet_index, has_header=False)
    arrearages = nwec.utility_reporting.arrearages.get_arrearages_df(df, num_months, COLS_PER_MONTH, KLI_SEARCH_STRING)

    date_row = nwec.utility_reporting.arrearages.infer_date_row(arrearages, source_date_format)
    arrearages = arrearages.tail(-date_row)  # remove rows before the date row
    arrearages = nwec.utility_reporting.arrearages.combine_arrearage_year_vintage_cols(
        arrearages, num_months, COLS_PER_MONTH
    )

    arrearages = nwec.utility_reporting.arrearages.normalize_vintage_cols(arrearages)
    arrearages = nwec.utility_reporting.arrearages.add_zip_and_customer_class_cols(df, arrearages)
    arrearages = nwec.utility_reporting.arrearages.normalize_arrearage_cols(arrearages, "Residential", Utility.AVISTA)
    arrearages = arrearages.filter(pl.col("Amount") > 0)

    assert (
        arrearages.select(pl.struct(["Zip Code", "Year", "Month", "Vintage"])).is_unique().all()
    ), "Duplicate rows detected"

    nwec.utils.combine_persisted_df(arrearages, CLEAN_UTILITY_DATA / "arrearage_amounts_kli.arrow", export_csv=True)
    return arrearages
