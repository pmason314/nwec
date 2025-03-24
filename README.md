# NWEC Data Analysis

Volunteer data analysis, pipeline automation, and scripting done for the Northwest Energy Coalition.


## Setup

1. Install [VS Code](https://code.visualstudio.com/) and [git](https://git-scm.com/downloads).
2. Set up git and SSH keys from the [GitHub instructions](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
3. Install `uv` for Python and dependency management:
    ```
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
5. `cd` to where you want this project folder to be and clone the repository:
    ```
    git clone git@github.com:pmason314/nwec.git
    ```
6. Open VS Code and open the repository folder from the previous step.
7. In the VS Code integrated terminal, create the project's virtual environment:
   ```
    uv venv --python-preference=only-managed
   ```
8. Install the project's dependencies:
    ```
    uv sync
    ```

## Usage
Run the following notebooks:
- [Combined arrearage amounts](./nwec/utility_reporting/arrearages/combined.ipynb)
- [Combined arrearage counts](./nwec/utility_reporting/arrearage_counts/combined.ipynb)

# Data Details
Data retrieved from https://www.utc.wa.gov/casedocket/2020/200281/docsets.

Avista (avista)
- COVID-19 Monthly Report for MM YYYY, on behalf of Avista Corporation
- 200281-AVA-COVID-19-Rpt-MM-DD-YYYY.xlsx
- Monthly reports have exactly the same naming as the quarterly reports, but the monthly ones don't have any info
- Only want the quarterly ones, so March/July/September/December (published the month after)
- Quarterly reports have everything from that calendar year, and blanks for the remaining months in that year

Cascade Natural Gas (cng)
- COVID-19 Monthly Report for ***, on behalf of Cascade Natural Gas Corporation
- 200281-CNGC-Qtrly-COVID-19-**.xlsx
- Revised COVID-19 Monthly Report for *****
- 200281-CNGC-Revised-Qtrly-COVID-19-***.xlsx
- All data ever is in their one xlsx file, separated by category and year

Northwest Natural Gas (nwng)
- COVID-19 Monthly Report for MM YYYY, on behalf of Northwest Natural Gas Company
- 200281-NWN-MM-YYYY-COVID-***.xlsx
- Files correctly have 1Q/2Q/3Q/4Q in the name, but submission titles aren't consistently named

PacifiCorp (pac)
- COVID-19 Monthly Report for September 2024, on behalf of PacifiCorp
- 200281-PAC-Q3-Covid-Rpt-10-30-24.xlsx

Puget Sound Energy (pse)
- COVID-19 Monthly Report for December 2024, on behalf of Puget Sound Energy
- U-200281-PSE-CLtr-(01-03-2025).pdf

# Q4 Reports
Avista and CNG have the whole year
NWNG, PAC, and PSE only have Q4

# Hi Charlee


