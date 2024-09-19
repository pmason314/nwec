"""Constants for the NWEC package."""

import nwec.utils

PROJECT_ROOT = nwec.utils.get_project_root()
DATA = PROJECT_ROOT / "data"
RAW_UTILITY_DATA = DATA / "utility_reporting" / "raw"
CLEAN_UTILITY_DATA = DATA / "utility_reporting" / "processed"


COMBINED_ARREARAGE_SCHEMA = [
    "Utility",
    "Zip Code",
    "Customer Class",
    "Year",
    "Month",
    "Vintage",
    "Amount",
]

COMBINED_ARREARAGE_COUNTS_SCHEMA = [
    "Utility",
    "Zip Code",
    "Year",
    "Month",
    "Vintage",
    "Count",
]
