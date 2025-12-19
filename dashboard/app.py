"""Dash web dashboard for utility reporting data visualization."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from dash import ALL, Dash, Input, Output, ctx, html

from dashboard.callbacks import create_dataset_callbacks
from dashboard.dashboard_config import get_dataset_configs, load_dataset
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
    title="Utility Reporting Dashboard",
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
    chips = []
    for util in all_utilities:
        is_selected = util in selected_utilities
        chips.append(
            html.Button(
                util,
                id={"type": "utility-chip", "index": util},
                n_clicks=0,
                style={
                    "padding": "10px 20px",
                    "margin": "5px",
                    "backgroundColor": "#156570" if is_selected else "white",
                    "color": "white" if is_selected else "#156570",
                    "border": "2px solid #156570",
                    "borderRadius": "25px",
                    "cursor": "pointer",
                    "fontSize": "14px",
                    "fontWeight": "500",
                    "transition": "all 0.3s ease",
                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)" if is_selected else "0 1px 3px rgba(0,0,0,0.05)",
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


# Create dynamic callbacks for all datasets
for config in dataset_configs:
    create_dataset_callbacks(app, config, all_utilities)


# Simple KPI cards callback with placeholders
@app.callback(
    Output("kpi-cards-container", "children"),
    Input("selected-utilities-store", "data"),
)
def update_kpi_cards(selected_utilities: list[str]) -> list:
    """Update KPI cards with simple placeholder values."""
    num_utilities = len(selected_utilities) if selected_utilities else 0

    return [
        html.Div(
            [
                html.Div("Total Records", className="kpi-card-title"),
                html.Div("10,000", className="kpi-card-value"),
                html.Div("Placeholder", className="kpi-card-subtitle"),
            ],
            className="kpi-card",
        ),
        html.Div(
            [
                html.Div("Active Utilities", className="kpi-card-title"),
                html.Div(str(num_utilities), className="kpi-card-value"),
                html.Div(f"of {len(all_utilities)} total", className="kpi-card-subtitle"),
            ],
            className="kpi-card",
        ),
        html.Div(
            [
                html.Div("Avg Monthly", className="kpi-card-title"),
                html.Div("500", className="kpi-card-value"),
                html.Div("Placeholder", className="kpi-card-subtitle"),
            ],
            className="kpi-card",
        ),
        html.Div(
            [
                html.Div("Unique Zips", className="kpi-card-title"),
                html.Div("50", className="kpi-card-value"),
                html.Div("Placeholder", className="kpi-card-subtitle"),
            ],
            className="kpi-card",
        ),
    ]


if __name__ == "__main__":
    # Run with `uv run --env-file .env gunicorn app:server -b 127.0.0.1:8080 -w 1` for production
    # Replace host IP with 0.0.0.0:8080 when running in dev
    # Use systemd for long term usage
    bind_host = os.environ.get("BIND_HOST", "localhost")
    port = int(os.environ.get("PORT", "8080"))
    app.run(debug=True, host=bind_host, port=port)
