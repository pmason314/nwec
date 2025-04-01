"""Constants for the NWEC package."""

from enum import Enum

import nwec.utils


class Utility(Enum):
    """List of utility companies participating in the disconnection moratorium."""

    AVISTA = ("Avista Corporation", "avista")
    CNG = ("Cascade Natural Gas Corporation", "cng")
    NWN = ("Northwest Natural Gas Company", "nwn")
    PAC = ("PacifiCorp", "pac")
    PSE = ("Puget Sound Energy", "pse")

    def __init__(self, full_name: str, code: str) -> None:
        self.full_name = full_name
        self.code = code


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
