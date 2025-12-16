"""Dash web dashboard for arrearage counts visualization."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from dash import ALL, Dash, Input, Output, ctx, html

from layouts.kpi_cards import build_kpi_cards
from layouts.main_layout import create_main_layout

# Load data
DATA_PATH = Path(__file__).parent / "data" / "utility_reporting" / "processed" / "arrearage_counts.arrow"
arrearage_counts = pl.read_ipc(DATA_PATH)

# Initialize the Dash app with external stylesheets
app = Dash(
    __name__,
    external_stylesheets=["https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"],
    title="Arrearage Counts Dashboard",
)
server = app.server  # Expose the server for deployment

# Get unique values for filters
all_utilities = sorted(arrearage_counts["Utility"].unique().to_list())
all_dates = sorted(arrearage_counts["Month"].unique().to_list())

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
available_years = sorted({date.year for date in all_dates})
available_months_by_year = {}
for year in available_years:
    available_months_by_year[year] = sorted({date.month for date in all_dates if date.year == year})

# Default start: first available month/year
start_year_default = all_dates[0].year
start_month_default = all_dates[0].month

# Default end: last available month/year
end_year_default = all_dates[-1].year
end_month_default = all_dates[-1].month

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


# Callback to update KPI cards, chart subtitle, chart, and table
@app.callback(
    [
        Output("kpi-cards", "children"),
        Output("chart-subtitle", "children"),
        Output("stacked-area-chart", "figure"),
        Output("data-table", "data"),
    ],
    [
        Input("start-month-picker", "value"),
        Input("start-year-picker", "value"),
        Input("end-month-picker", "value"),
        Input("end-year-picker", "value"),
        Input("selected-utilities-store", "data"),
    ],
)
def update_dashboard(
    start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
) -> tuple[list[html.Div], str, go.Figure, list[dict]]:
    """Update the KPI cards, chart and table based on filter selections."""
    # Convert month/year to datetime objects
    start_date = datetime(start_year, start_month, 1, tzinfo=UTC) if start_month and start_year else all_dates[0]
    end_date = datetime(end_year, end_month, 1, tzinfo=UTC) if end_month and end_year else all_dates[-1]

    # Ensure we have a list of utilities - if empty, show NO data
    if not selected_utilities:
        selected_utilities = []

    # Filter by date range first
    filtered_df = arrearage_counts.filter((pl.col("Month") >= start_date) & (pl.col("Month") <= end_date))

    # Then filter by utilities - if none selected, return empty dataframe
    if selected_utilities:
        filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
    else:
        # No utilities selected - return empty dataframe
        filtered_df = filtered_df.filter(pl.lit(value=False))

    # Calculate KPI metrics
    total_arrearages = filtered_df.select(pl.col("Arrearage Count").sum()).item()
    avg_monthly = (
        filtered_df.group_by("Month")
        .agg(pl.col("Arrearage Count").sum())
        .select(pl.col("Arrearage Count").mean())
        .item()
    )
    unique_zips = filtered_df.select(pl.col("Zip Code").n_unique()).item()
    num_utilities = len(selected_utilities) if selected_utilities else 0

    # Handle None values when filtered_df is empty
    total_arrearages = total_arrearages if total_arrearages is not None else 0
    avg_monthly = avg_monthly if avg_monthly is not None else 0
    unique_zips = unique_zips if unique_zips is not None else 0

    # Calculate trend (compare first and last month)
    monthly_totals = filtered_df.group_by("Month").agg(pl.col("Arrearage Count").sum()).sort("Month")
    if len(monthly_totals) >= 2:
        first_month = monthly_totals[0, "Arrearage Count"]
        last_month = monthly_totals[-1, "Arrearage Count"]
        percent_change = ((last_month - first_month) / first_month * 100) if first_month > 0 else 0
    else:
        percent_change = 0

    # Create KPI cards using the component builder
    kpi_cards = build_kpi_cards(
        total_arrearages=total_arrearages,
        avg_monthly=avg_monthly,
        unique_zips=unique_zips,
        num_utilities=num_utilities,
        total_utilities=len(all_utilities),
        percent_change=percent_change,
        num_months=len(monthly_totals),
    )

    # Create chart subtitle
    chart_subtitle = f"Showing data from {start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"

    # Aggregate data for the stacked area chart
    chart_data = filtered_df.group_by(["Month", "Utility"]).agg(pl.col("Arrearage Count").sum()).sort("Month")

    # Convert to pandas for Plotly
    chart_df = chart_data.to_pandas()

    # Create stacked area chart
    fig = go.Figure()

    colors = {"PSE": "#156570", "Avista": "#B4CEB3", "PAC": "#9B7EDE", "CNG": "#FE5F55", "NWN": "#5C415D"}

    # Add traces for each utility
    for utility in selected_utilities if selected_utilities else all_utilities:
        utility_data = chart_df[chart_df["Utility"] == utility].sort_values("Month")
        if not utility_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=utility_data["Month"],
                    y=utility_data["Arrearage Count"],
                    name=utility,
                    mode="lines",
                    stackgroup="one",
                    fillcolor=colors.get(utility, "#cccccc"),
                    line={"width": 0.5, "color": colors.get(utility, "#cccccc")},
                    hovertemplate=f"<b>{utility}</b><br>%{{y:,.0f}}<extra></extra>",
                )
            )

    fig.update_layout(
        xaxis_title="",
        yaxis_title="Arrearage Count",
        legend={
            "title": {"text": "Utility", "font": {"size": 14, "weight": 600}},
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": 1.02,
        },
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=550,
        margin={"l": 60, "r": 140, "t": 20, "b": 60},
        xaxis={
            "tickformat": "%b-%y",
            "showgrid": True,
            "gridcolor": "#e1e8ed",
            "gridwidth": 1,
            "tickfont": {"size": 12},
        },
        yaxis={
            "showgrid": True,
            "gridcolor": "#e1e8ed",
            "gridwidth": 1,
            "tickformat": ",.0f",
            "tickfont": {"size": 12},
        },
        font={"family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"},
    )

    # Prepare table data
    table_df = filtered_df.to_pandas()
    table_df["Month"] = table_df["Month"].dt.strftime("%b %Y")
    table_data = table_df.to_dict("records")

    return kpi_cards, chart_subtitle, fig, table_data


# Callback for CSV download
@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("download-btn", "n_clicks"),
    [
        Input("start-month-picker", "value"),
        Input("start-year-picker", "value"),
        Input("end-month-picker", "value"),
        Input("end-year-picker", "value"),
        Input("selected-utilities-store", "data"),
    ],
    prevent_initial_call=True,
)
def download_csv(
    n_clicks: int | None,
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    selected_utilities: list[str],
) -> dict | None:
    """Download the filtered data as CSV."""
    if n_clicks is None or n_clicks == 0:
        return None

    # Convert month/year to datetime objects
    start_date = datetime(start_year, start_month, 1, tzinfo=UTC) if start_month and start_year else all_dates[0]
    end_date = datetime(end_year, end_month, 1, tzinfo=UTC) if end_month and end_year else all_dates[-1]

    # Ensure we have a list of utilities - if empty, show NO data
    if not selected_utilities:
        selected_utilities = []

    # Filter by date range first
    filtered_df = arrearage_counts.filter((pl.col("Month") >= start_date) & (pl.col("Month") <= end_date))

    # Then filter by utilities - if none selected, return empty dataframe
    if selected_utilities:
        filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
    else:
        # No utilities selected - return empty dataframe
        filtered_df = filtered_df.filter(pl.lit(value=False))

    # Convert to pandas and prepare for download
    table_df = filtered_df.to_pandas()
    table_df["Month"] = table_df["Month"].dt.strftime("%b %Y")
    csv_string = table_df.to_csv(index=False)

    return {"content": csv_string, "filename": "arrearage_counts_export.csv"}


if __name__ == "__main__":
    # Run with `uv run --env-file .env gunicorn app:server -b 127.0.0.1:8080 -w 1` for production
    # Use systemd for long term usage
    bind_host = os.environ.get("BIND_HOST", "localhost")
    port = int(os.environ.get("PORT", "8080"))
    app.run(debug=True, host=bind_host, port=port)
