"""Dash web dashboard for utility reporting data visualization."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from dash import ALL, Dash, Input, Output, ctx, html

from dashboard.callbacks import create_dataset_callbacks
from dashboard.callbacks_bill_assistance import create_bill_assistance_callbacks
from dashboard.callbacks_collections import create_collections_callbacks
from dashboard.callbacks_disconnections import create_disconnections_callbacks
from dashboard.callbacks_past_due_balances import create_past_due_balances_callbacks
from dashboard.dashboard_config import (
    UTILITY_COLORS,
    UTILITY_DISPLAY_NAMES,
    get_dataset_configs,
    load_dataset,
)
from dashboard.layouts.main_layout import create_main_layout

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
server = app.server  # Expose the server for deployment

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
create_bill_assistance_callbacks(app, all_utilities)
create_collections_callbacks(app, all_utilities)


# KPI cards callback to show statewide metrics
@app.callback(
    Output("kpi-cards-container", "children"),
    [
        Input("start-month-picker", "value"),
        Input("start-year-picker", "value"),
        Input("end-month-picker", "value"),
        Input("end-year-picker", "value"),
        Input("selected-utilities-store", "data"),
    ],
)
def update_kpi_cards(
    start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
) -> list:
    """Update KPI cards with statewide metrics based on filters."""
    from dashboard.layouts.kpi_cards import build_kpi_cards

    # Convert month/year to datetime objects
    start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
    end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

    # Create date range text for subtitle
    date_range_text = f"{start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')}"

    # If no utilities selected, show zeros
    if not selected_utilities:
        return build_kpi_cards(
            total_customers_with_arrearages=0,
            total_arrearage_amount=0.0,
            total_disconnections=0,
            total_bill_assistance=0.0,
            date_range_text=date_range_text,
        )

    # Load datasets
    arrearage_counts = load_dataset("arrearage_counts").with_columns(
        pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
    )
    arrearage_amounts = load_dataset("arrearage_amounts").with_columns(
        pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
    )
    disconnections = load_dataset("disconnections").with_columns(
        pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
    )
    bill_assist = load_dataset("bill_assist").with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))

    # Filter by date range and utilities
    arrearage_counts_filtered = arrearage_counts.filter(
        (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date) & pl.col("Utility").is_in(selected_utilities)
    )
    arrearage_amounts_filtered = arrearage_amounts.filter(
        (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date) & pl.col("Utility").is_in(selected_utilities)
    )
    disconnections_filtered = disconnections.filter(
        (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date) & pl.col("Utility").is_in(selected_utilities)
    )
    bill_assist_filtered = bill_assist.filter(
        (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date) & pl.col("Utility").is_in(selected_utilities)
    )

    # Calculate totals
    total_customers = arrearage_counts_filtered.select(pl.col("Arrearage Customer Count").sum()).item()
    total_amount = arrearage_amounts_filtered.select(pl.col("Arrearage_Amount").sum()).item()
    total_disconnects = disconnections_filtered.select(pl.col("Number of Disconnects").sum()).item()
    total_assist = bill_assist_filtered.select(pl.col("Bill Assist Customer Count").sum()).item()

    # Handle None values
    total_customers = total_customers if total_customers is not None else 0
    total_amount = total_amount if total_amount is not None else 0.0
    total_disconnects = total_disconnects if total_disconnects is not None else 0
    total_assist = total_assist if total_assist is not None else 0.0

    return build_kpi_cards(
        total_customers_with_arrearages=total_customers,
        total_arrearage_amount=total_amount,
        total_disconnections=total_disconnects,
        total_bill_assistance=total_assist,
        date_range_text=date_range_text,
    )


if __name__ == "__main__":
    # Run with `uv run --env-file .env gunicorn app:server -b 127.0.0.1:8080 -w 1` for production
    # Replace host IP with 0.0.0.0:8080 when running in dev
    # Use systemd for long term usage
    bind_host = os.environ.get("BIND_HOST", "localhost")
    port = int(os.environ.get("PORT", "8080"))
    app.run(debug=True, host=bind_host, port=port)
