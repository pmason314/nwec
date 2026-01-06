"""Utility functions for dashboard data processing and filtering."""

from datetime import UTC, datetime

import polars as pl
from scipy import stats

# Utility button colors
UTILITY_COLORS = {
    "Avista": "#003768",
    "PSE": "#2596be",
    "PAC": "#e90028",
    "CNG": "#6c757d",
    "NWN": "#54b266",
}


def filter_by_date_and_utilities(
    df: pl.DataFrame,
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    selected_utilities: list[str] | None = None,
) -> pl.DataFrame:
    """Filter dataframe by date range and utilities.

    Args:
        df: Input dataframe with Year and Month columns
        start_month: Starting month (1-12)
        start_year: Starting year
        end_month: Ending month (1-12)
        end_year: Ending year
        selected_utilities: List of utility names to filter by. If None or empty,
                          returns empty dataframe

    Returns:
        Filtered dataframe with Date column added
    """
    if not selected_utilities:
        selected_utilities = []

    # Create date range
    start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
    end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

    # Add Date column and filter by date range
    df_filtered = df.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
        (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
    )

    # Filter by utilities
    if selected_utilities:
        df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
    else:
        # Return empty dataframe if no utilities selected
        df_filtered = df_filtered.filter(pl.lit(value=False))

    return df_filtered


def aggregate_by_utility_date(
    df: pl.DataFrame,
    value_column: str,
    aggregation: str = "sum",
) -> pl.DataFrame:
    """Aggregate data by utility and date.

    Args:
        df: Input dataframe with Utility, Date, and value columns
        value_column: Name of the column to aggregate
        aggregation: Type of aggregation ('sum', 'mean', 'count', etc.)

    Returns:
        Aggregated dataframe sorted by Utility and Date
    """
    agg_expr = getattr(pl.col(value_column), aggregation)()

    return df.group_by(["Utility", "Date"]).agg(agg_expr).sort(["Utility", "Date"])


def prepare_table_data(
    df: pl.DataFrame,
    date_format: str = "%b %Y",
    sort_columns: list[str] | None = None,
) -> list[dict]:
    """Prepare filtered data for dash table display.

    Args:
        df: Input dataframe with Date column
        date_format: Format string for date display (default: "Jan 2023")
        sort_columns: List of columns to sort by. If None, uses ["Utility", "Date", "Zip Code"]

    Returns:
        List of dictionaries ready for dash DataTable
    """
    if sort_columns is None:
        sort_columns = ["Utility", "Date", "Zip Code"]

    # Sort by Date column first (chronological order)
    existing_sort_cols = [col for col in sort_columns if col in df.columns]
    if existing_sort_cols:
        df = df.sort(existing_sort_cols)

    # Convert Date to ISO format string for sortable representation in table
    # Add formatted Month column for display
    df_formatted = df.with_columns(
        [
            pl.col("Date").dt.strftime("%Y-%m-%d").alias("Date"),
            pl.col("Date").dt.strftime(date_format).alias("Month"),
        ]
    )

    return df_formatted.to_dicts()


def calculate_trendline(
    dates: list[datetime],
    y_values: list[float],
) -> tuple[list[float], float, float]:
    """Calculate linear trendline using scipy.

    Args:
        dates: List of datetime objects
        y_values: List of y-axis values

    Returns:
        Tuple of (trendline_y_values, slope, r_squared)
    """
    # Convert dates to numeric (days from first date)
    x_numeric = [(d - dates[0]).days for d in dates]

    # Calculate linear regression
    slope, intercept, r_value, _, _ = stats.linregress(x_numeric, y_values)

    # Generate trendline values
    trendline_y = [slope * x + intercept for x in x_numeric]

    return trendline_y, slope, r_value**2


def format_currency(value: float) -> str:
    """Format value as currency string."""
    return f"${value:,.0f}"


def format_number(value: float) -> str:
    """Format value as number with thousands separator."""
    return f"{value:,.0f}"


def get_subtitle_text(
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    selected_utilities: list[str],
    utility_display_names: dict[str, str],
) -> str:
    """Generate subtitle text for charts.

    Args:
        start_month: Starting month (1-12)
        start_year: Starting year
        end_month: Ending month (1-12)
        end_year: Ending year
        selected_utilities: List of selected utility names
        utility_display_names: Mapping of utility codes to display names

    Returns:
        Formatted subtitle string
    """
    # Format date range
    start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
    end_date = datetime(end_year, end_month, 1, tzinfo=UTC)
    date_range = f"{start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')}"

    # Format utilities
    if not selected_utilities:
        utilities_text = "No utilities selected"
    elif len(selected_utilities) == 1:
        utilities_text = utility_display_names.get(selected_utilities[0], selected_utilities[0])
    else:
        utilities_text = f"{len(selected_utilities)} utilities"

    return f"{utilities_text} | {date_range}"
