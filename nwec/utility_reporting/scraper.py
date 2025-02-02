import re
import warnings
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from nwec.constants import RAW_UTILITY_DATA, Utility
from nwec.utils import get_previous_quarter

DOCKET_URL = "https://www.utc.wa.gov/casedocket/2020/200281/docsets"
submission_title = "COVID-19 Monthly Report for December 2024, on behalf of Cascade Natural Gas Corporation"


def download_latest(
    utility: Utility | Iterable[Utility] | None = None, submission_start_date: date | None = None
) -> None:
    """Download the latest reports from the UTC docket."""
    if isinstance(utility, Utility):
        utilities = [utility]
    elif utility is None:
        utilities = list(Utility.__members__.values())
    else:
        utilities = utility

    submission_title_patterns = get_submission_title_patterns(utilities)

    for utility_company in utilities:
        submission_title = submission_title_patterns[utility_company]
        response = requests.get(DOCKET_URL, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        submission = soup.find_all(string=re.compile(submission_title))[0]
        submission_date = submission.parent.parent.find_all("td")[1].text
        submission_date = datetime.strptime(submission_date, "%m/%d/%Y").replace(tzinfo=UTC).date()

        if submission_start_date and submission_start_date > submission_date:
            warnings.warn(
                f"Skipping latest {utility_company.full_name} report that was published on {submission_date}",
                stacklevel=2,
            )
            continue

        report_link = submission.parent.find("a", string=re.compile("xlsx"))
        download_link = report_link["href"]
        report_file_name = report_link.text

        xlsx_report_download = requests.get(download_link, timeout=20)
        xlsx_report_download.raise_for_status()

        with Path(RAW_UTILITY_DATA / "downloads" / f"{utility_company.code}_{report_file_name}").open("wb") as file:
            file.write(xlsx_report_download.content)
        print(
            f"Downloaded {utility_company.code}_{report_file_name} for {utility_company.full_name} "
            f"(published on {submission_date})"
        )


def get_submission_title_patterns(utilities: Iterable[Utility]) -> dict[Utility, re.Pattern]:
    """Get the submission title patterns for the given utilities."""
    submission_title_patterns = {}
    base_pattern = r"COVID-19 Monthly( Q\d)? Report for \w+ \d{4}, on behalf of "
    for utility in utilities:
        submission_title_patterns[utility] = re.compile(base_pattern + f"{utility.full_name}")
    return submission_title_patterns


def rename_downloads(cutoff_date: date | None = None) -> None:
    """Rename the downloaded files to include the utility name."""
    if cutoff_date is None:
        cutoff_date = datetime.now(tz=UTC).date()
    quarter, year = get_previous_quarter(cutoff_date)
    for file in Path(RAW_UTILITY_DATA / "downloads").iterdir():
        if file.is_file():
            utility = file.name.split("_")[0]

            file.rename(RAW_UTILITY_DATA / str(year) / f"{utility}_{year}_Q{quarter}.{file.suffix}")


rename_downloads(date(2025, 1, 1))
