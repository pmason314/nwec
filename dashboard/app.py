"""Dash web dashboard for utility reporting data visualization."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from dash import ALL, Dash, Input, Output, ctx, html

from dashboard.callbacks.dataset_callbacks import create_dataset_callbacks
from dashboard.callbacks.tab_callbacks_bill_assistance import create_bill_assistance_callbacks
from dashboard.callbacks.tab_callbacks_collections import create_collections_callbacks
from dashboard.callbacks.tab_callbacks_disconnections import create_disconnections_callbacks
from dashboard.callbacks.tab_callbacks_past_due_balances import create_past_due_balances_callbacks
from dashboard.dashboard_config import (
    UTILITY_DISPLAY_NAMES,
    get_dataset_configs,
    load_dataset,
)
from dashboard.layouts.kpi_cards import MetricComparison, build_kpi_cards
from dashboard.layouts.main_layout import create_main_layout
from dashboard.utils import UTILITY_COLORS

# Get all available datasets
dataset_configs = get_dataset_configs()

# Load all datasets to get the complete range of utilities and dates
all_utilities_set = set()
all_years_set = set()
all_year_month_combinations = set()

for config in dataset_configs:
    dataset = load_dataset(config.file_name)
    all_utilities_set.update(dataset["Utility"].unique().to_list())
    all_years_set.update(dataset["Year"].unique().to_list())
    # Store year-month pairs
    year_month_pairs = dataset.select(["Year", "Month"]).unique().to_dicts()
    for pair in year_month_pairs:
        all_year_month_combinations.add((pair["Year"], pair["Month"]))

# Sort utilities and years
all_utilities = sorted(all_utilities_set)

# Load first dataset for KPI cards
first_dataset = load_dataset(dataset_configs[0].file_name)

# Initialize the Dash app with external stylesheets
# Point to assets folder in parent directory
assets_path = Path(__file__).parent.parent / "assets"
app = Dash(
    __name__,
    external_stylesheets=["https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"],
    title="Energy Affordability Dashboard",
    assets_folder=str(assets_path),
)

# Build available_months_by_year from all datasets
available_years = sorted(all_years_set)
available_months_by_year = {}
for year in available_years:
    months_in_year = [month for y, month in all_year_month_combinations if y == year]
    available_months_by_year[year] = sorted(months_in_year)

# Create month and year options for dropdowns
month_names = [
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

# Default start: earliest available month/year across all datasets
sorted_combinations = sorted(all_year_month_combinations)
start_year_default, start_month_default = sorted_combinations[0]

# Default end: latest available month/year across all datasets
end_year_default, end_month_default = sorted_combinations[-1]

# Create the layout using modular components
app.layout = create_main_layout(
    all_utilities,
    available_months_by_year,
    available_years,
    start_year_default,
    start_month_default,
    end_year_default,
    end_month_default,
    month_names,
)


# Callback to handle chip selection
@app.callback(
    [Output("selected-utilities-store", "data"), Output("utility-chips-container", "children")],
    [
        Input({"type": "utility-chip", "index": ALL}, "n_clicks"),
        Input("select-all-btn", "n_clicks"),
        Input("clear-all-btn", "n_clicks"),
    ],
    [Input("selected-utilities-store", "data")],
    prevent_initial_call=True,
)
def update_utility_selection(
    _chip_clicks: list[int],
    _select_all: int,
    _clear_all: int,
    current_selection: list,
) -> tuple[list[str], list]:
    """Handle utility chip selection and Select All/Clear All buttons."""
    if not ctx.triggered:
        return current_selection, create_chips(current_selection)

    button_id = ctx.triggered[0]["prop_id"]

    # Handle Select All button
    if "select-all-btn" in button_id:
        return all_utilities, create_chips(all_utilities)

    # Handle Clear All button
    if "clear-all-btn" in button_id:
        return [], create_chips([])

    # Handle individual chip click
    if "utility-chip" in button_id:
        # Extract which chip was clicked
        prop_id_dict = json.loads(button_id.split(".")[0])
        clicked_util = prop_id_dict["index"]

        # Toggle the utility in the selection
        new_selection = list(current_selection) if current_selection else []
        if clicked_util in new_selection:
            new_selection.remove(clicked_util)
        else:
            new_selection.append(clicked_util)

        return new_selection, create_chips(new_selection)

    return current_selection, create_chips(current_selection)


def create_chips(selected_utilities: list[str]) -> list:
    """Create chip components with proper styling based on selection state."""
    # Sort utilities by display name length (shortest first)
    sorted_utilities = sorted(
        all_utilities,
        key=lambda u: len(UTILITY_DISPLAY_NAMES.get(u, u)),
    )

    chips = []
    for util in sorted_utilities:
        is_selected = util in selected_utilities
        util_color = UTILITY_COLORS.get(util, "#003768")
        display_name = UTILITY_DISPLAY_NAMES.get(util, util)
        chips.append(
            html.Button(
                display_name,
                id={"type": "utility-chip", "index": util},
                n_clicks=0,
                style={
                    "padding": "10px 20px",
                    "margin": "5px 0",
                    "backgroundColor": util_color if is_selected else "white",
                    "color": "white" if is_selected else util_color,
                    "border": f"2px solid {util_color}",
                    "borderRadius": "25px",
                    "cursor": "pointer",
                    "fontSize": "14px",
                    "fontWeight": "500",
                    "transition": "all 0.3s ease",
                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)" if is_selected else "0 1px 3px rgba(0,0,0,0.05)",
                    "display": "inline-block",
                },
            )
        )
    return chips


# Callback to update month options when year changes for start date
@app.callback(
    Output("start-month-picker", "options"),
    Input("start-year-picker", "value"),
)
def update_start_month_options(selected_year: int) -> list[dict]:
    """Update available months based on selected year for start date."""
    if selected_year in available_months_by_year:
        return [{"label": month_names[i - 1], "value": i} for i in available_months_by_year[selected_year]]
    return [{"label": month_names[i - 1], "value": i + 1} for i in range(12)]


# Callback to update month options when year changes for end date
@app.callback(
    Output("end-month-picker", "options"),
    Input("end-year-picker", "value"),
)
def update_end_month_options(selected_year: int) -> list[dict]:
    """Update available months based on selected year for end date."""
    if selected_year in available_months_by_year:
        return [{"label": month_names[i - 1], "value": i} for i in available_months_by_year[selected_year]]
    return [{"label": month_names[i - 1], "value": i + 1} for i in range(12)]


# Create dynamic callbacks for all datasets (excluding datasets with comprehensive tabs)
excluded_datasets = {
    "arrearage_counts",
    "arrearage_amounts",
    "kli_arrearage_amounts",
    "disconnections",
    "disconnection_notices",
    "bill_assist",
    "payment_agreements",
    "collection_agency_referrals",
}

for config in dataset_configs:
    # Skip datasets that have comprehensive tabs
    if config.file_name not in excluded_datasets:
        create_dataset_callbacks(app, config, all_utilities)

# Create callbacks for comprehensive tabs
create_past_due_balances_callbacks(app, all_utilities)
create_disconnections_callbacks(app, all_utilities)
create_bill_assistance_callbacks(app)
create_collections_callbacks(app)


# Helper functions for KPI calculations
def _load_kpi_datasets() -> dict[str, pl.DataFrame]:
    """Load and prepare all datasets needed for KPI calculations."""
    return {
        "arrearage_counts": load_dataset("arrearage_counts").with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ),
        "arrearage_amounts": load_dataset("arrearage_amounts").with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ),
        "disconnections": load_dataset("disconnections").with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ),
        "assistance_liheap": load_dataset("assistance_liheap").with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ),
        "assistance_utility": load_dataset("assistance_utility").with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ),
    }


def _get_quarter_boundaries(month: int, year: int) -> tuple[datetime, datetime, int]:
    """Get quarter start, end dates, and quarter number for a given month/year."""
    if month <= 3:  # Q1
        return datetime(year, 1, 1, tzinfo=UTC), datetime(year, 3, 1, tzinfo=UTC), 1
    if month <= 6:  # Q2
        return datetime(year, 4, 1, tzinfo=UTC), datetime(year, 6, 1, tzinfo=UTC), 2
    if month <= 9:  # Q3
        return datetime(year, 7, 1, tzinfo=UTC), datetime(year, 9, 1, tzinfo=UTC), 3
    # Q4
    return datetime(year, 10, 1, tzinfo=UTC), datetime(year, 12, 1, tzinfo=UTC), 4


def _get_previous_quarter_boundaries(quarter_num: int, year: int) -> tuple[datetime, datetime]:
    """Get previous quarter start and end dates."""
    if quarter_num == 1:
        return datetime(year - 1, 10, 1, tzinfo=UTC), datetime(year - 1, 12, 1, tzinfo=UTC)
    prev_q_start_month = (quarter_num - 2) * 3 + 1
    prev_q_end_month = (quarter_num - 1) * 3
    return datetime(year, prev_q_start_month, 1, tzinfo=UTC), datetime(year, prev_q_end_month, 1, tzinfo=UTC)


def _calculate_quarter_metrics(
    datasets: dict[str, pl.DataFrame],
    q_start: datetime,
    q_end: datetime,
    selected_utilities: list[str],
) -> tuple[float, float, float, float]:
    """Calculate customer count, amount, disconnections, and assistance for a quarter."""
    # Filter datasets for the quarter
    counts_q = datasets["arrearage_counts"].filter(
        (pl.col("Date") >= q_start) & (pl.col("Date") <= q_end) & pl.col("Utility").is_in(selected_utilities)
    )
    amounts_q = datasets["arrearage_amounts"].filter(
        (pl.col("Date") >= q_start) & (pl.col("Date") <= q_end) & pl.col("Utility").is_in(selected_utilities)
    )
    disconnects_q = datasets["disconnections"].filter(
        (pl.col("Date") >= q_start) & (pl.col("Date") <= q_end) & pl.col("Utility").is_in(selected_utilities)
    )
    liheap_q = datasets["assistance_liheap"].filter(
        (pl.col("Date") >= q_start) & (pl.col("Date") <= q_end) & pl.col("Utility").is_in(selected_utilities)
    )
    utility_assist_q = datasets["assistance_utility"].filter(
        (pl.col("Date") >= q_start) & (pl.col("Date") <= q_end) & pl.col("Utility").is_in(selected_utilities)
    )

    # For customer counts, use the most recent month in the quarter (snapshot data)
    max_date_in_q = counts_q.select(pl.col("Date").max()).item()
    if max_date_in_q:
        customers = (
            counts_q.filter(pl.col("Date") == max_date_in_q).select(pl.col("Arrearage Customer Count").sum()).item()
            or 0
        )
    else:
        customers = 0

    # For other metrics, sum across the quarter (flow data)
    amount = amounts_q.select(pl.col("Arrearage_Amount").sum()).item() or 0.0
    disconnects = disconnects_q.select(pl.col("Number of Disconnects").sum()).item() or 0
    liheap = liheap_q.select(pl.col("Assistance Amount").sum()).item() or 0.0
    utility_assist = utility_assist_q.select(pl.col("Assistance Amount").sum()).item() or 0.0
    assist = liheap + utility_assist

    return customers, amount, disconnects, assist


def _format_comparison(current: float, previous: float, is_negative_good: bool = False) -> tuple[str, str, str]:
    """Format comparison text, arrow, and color."""
    if previous == 0:
        if current == 0:
            return "No change", "→", "#95a5a6"
        change_text = f"+{current:,.0f}" if current < 1_000_000 else f"+${current / 1_000_000:.1f}M"
        arrow = "↑"
        color = "#e74c3c" if is_negative_good else "#27ae60"
        return change_text, arrow, color

    change = current - previous
    pct_change = (change / previous) * 100

    # Format change text
    if abs(current) >= 1_000_000:
        if change < 0:
            change_text = f"-${abs(change) / 1_000_000:.1f}M ({pct_change:.1f}%)"
        else:
            change_text = f"+${change / 1_000_000:.1f}M ({pct_change:+.1f}%)"
    elif abs(current) >= 1_000:
        if change < 0:
            change_text = f"-{abs(change) / 1_000:.1f}K ({pct_change:.1f}%)"
        else:
            change_text = f"+{change / 1_000:.1f}K ({pct_change:+.1f}%)"
    else:
        change_text = f"{change:+,.0f} ({pct_change:+.1f}%)"

    if change > 0:
        arrow, color = "↑", ("#e74c3c" if is_negative_good else "#27ae60")
    elif change < 0:
        arrow, color = "↓", ("#27ae60" if is_negative_good else "#e74c3c")
    else:
        arrow, color = "→", "#95a5a6"

    return change_text, arrow, color


# KPI cards callback to show statewide metrics for the most recent quarter
@app.callback(
    Output("kpi-cards-container", "children"),
    Input("selected-utilities-store", "data"),
)
def update_kpi_cards(selected_utilities: list[str]) -> list:
    """Update KPI cards with most recent quarter metrics (independent of date filters)."""
    # If no utilities selected, use all utilities
    if not selected_utilities:
        selected_utilities = all_utilities

    # Load datasets
    datasets = _load_kpi_datasets()

    # Find the most recent date across all datasets
    max_dates = [df.select(pl.col("Date").max()).item() for df in datasets.values()]
    most_recent_date = max(d for d in max_dates if d is not None)

    # Determine current quarter boundaries
    current_q_start, current_q_end, quarter_num = _get_quarter_boundaries(most_recent_date.month, most_recent_date.year)

    # Previous quarter boundaries
    prev_q_start, prev_q_end = _get_previous_quarter_boundaries(quarter_num, most_recent_date.year)

    # Same quarter last year boundaries
    yoy_q_start = datetime(most_recent_date.year - 1, current_q_start.month, 1, tzinfo=UTC)
    yoy_q_end = datetime(most_recent_date.year - 1, current_q_end.month, 1, tzinfo=UTC)

    # Calculate totals for all three periods
    current_metrics = _calculate_quarter_metrics(datasets, current_q_start, current_q_end, selected_utilities)
    prev_metrics = _calculate_quarter_metrics(datasets, prev_q_start, prev_q_end, selected_utilities)
    yoy_metrics = _calculate_quarter_metrics(datasets, yoy_q_start, yoy_q_end, selected_utilities)

    current_customers, current_amount, current_disconnects, current_assist = current_metrics
    prev_customers, prev_amount, prev_disconnects, prev_assist = prev_metrics
    yoy_customers, yoy_amount, yoy_disconnects, yoy_assist = yoy_metrics

    # Calculate QoQ and YoY comparisons for each metric
    customers_qoq = _format_comparison(current_customers, prev_customers, is_negative_good=True)
    customers_yoy = _format_comparison(current_customers, yoy_customers, is_negative_good=True)
    amount_qoq = _format_comparison(current_amount, prev_amount, is_negative_good=True)
    amount_yoy = _format_comparison(current_amount, yoy_amount, is_negative_good=True)
    disconnects_qoq = _format_comparison(current_disconnects, prev_disconnects, is_negative_good=True)
    disconnects_yoy = _format_comparison(current_disconnects, yoy_disconnects, is_negative_good=True)
    assist_qoq = _format_comparison(current_assist, prev_assist, is_negative_good=False)
    assist_yoy = _format_comparison(current_assist, yoy_assist, is_negative_good=False)

    # Format date range for display
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    start_month_name = month_names[current_q_start.month - 1]
    end_month_name = month_names[current_q_end.month - 1]
    date_range_text = f"Q{quarter_num} {most_recent_date.year} ({start_month_name}-{end_month_name})"

    # Build metric comparison objects
    return build_kpi_cards(
        customers=MetricComparison(
            value=current_customers,
            qoq_change=customers_qoq[0],
            qoq_arrow=customers_qoq[1],
            qoq_color=customers_qoq[2],
            yoy_change=customers_yoy[0],
            yoy_arrow=customers_yoy[1],
            yoy_color=customers_yoy[2],
        ),
        amount=MetricComparison(
            value=current_amount,
            qoq_change=amount_qoq[0],
            qoq_arrow=amount_qoq[1],
            qoq_color=amount_qoq[2],
            yoy_change=amount_yoy[0],
            yoy_arrow=amount_yoy[1],
            yoy_color=amount_yoy[2],
        ),
        disconnections=MetricComparison(
            value=current_disconnects,
            qoq_change=disconnects_qoq[0],
            qoq_arrow=disconnects_qoq[1],
            qoq_color=disconnects_qoq[2],
            yoy_change=disconnects_yoy[0],
            yoy_arrow=disconnects_yoy[1],
            yoy_color=disconnects_yoy[2],
        ),
        assistance=MetricComparison(
            value=current_assist,
            qoq_change=assist_qoq[0],
            qoq_arrow=assist_qoq[1],
            qoq_color=assist_qoq[2],
            yoy_change=assist_yoy[0],
            yoy_arrow=assist_yoy[1],
            yoy_color=assist_yoy[2],
        ),
        date_range=date_range_text,
    )


# Expose the server for deployment (MUST be after all callbacks are registered in Dash 4.0+)
server = app.server

if __name__ == "__main__":
    # Run with `uv run --env-file .env gunicorn app:server -b 127.0.0.1:8080 -w 1` for production
    # Replace host IP with 0.0.0.0:8080 when running in dev
    # Use systemd for long term usage
    bind_host = os.environ.get("BIND_HOST", "localhost")
    port = int(os.environ.get("PORT", "8080"))
    app.run(debug=True, host=bind_host, port=port)
