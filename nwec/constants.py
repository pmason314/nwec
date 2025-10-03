"""Constants for the NWEC package."""

from enum import Enum

import nwec.utils


class Utility(Enum):
    """List of utility companies participating in the disconnection moratorium."""

    PSE = ("Puget Sound Energy", "PSE")
    AVISTA = ("Avista Corporation", "Avista")
    PAC = ("PacifiCorp", "PAC")
    CNG = ("Cascade Natural Gas Corporation", "CNG")
    NWN = ("Northwest Natural Gas Company", "NWN")

    def __init__(self, full_name: str, code: str) -> None:
        self.full_name = full_name
        self.code = code


PROJECT_ROOT = nwec.utils.get_project_root()
DATA = PROJECT_ROOT / "data"
RAW_UTILITY_DATA = DATA / "utility_reporting" / "raw"
CLEAN_UTILITY_DATA = DATA / "utility_reporting" / "processed"
REPORTS = DATA / "utility_reporting" / "reports"

MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

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
