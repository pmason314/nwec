"""Dash web dashboard for arrearage counts visualization."""

import json
import os
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from dash import ALL, Dash, Input, Output, ctx, dash_table, dcc, html

# Load data
DATA_PATH = Path(__file__).parent / "data" / "utility_reporting" / "processed" / "arrearage_counts.arrow"
arrearage_counts = pl.read_ipc(DATA_PATH)

# Initialize the Dash app with external stylesheets
app = Dash(
    __name__, external_stylesheets=["https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"]
)
server = app.server  # Expose the server for deployment

# Custom CSS for professional dropdowns
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            }
            
            /* Professional dropdown styling */
            .date-dropdown .Select-control {
                border: 2px solid #e0e0e0 !important;
                border-radius: 8px !important;
                background-color: white !important;
                box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05) !important;
                transition: all 0.2s ease !important;
                height: 42px !important;
                font-size: 14px !important;
            }
            
            .date-dropdown .Select-control:hover {
                border-color: #2563eb !important;
                box-shadow: 0 2px 8px rgba(37, 99, 235, 0.15) !important;
            }
            
            .date-dropdown .is-focused .Select-control {
                border-color: #2563eb !important;
                box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
            }
            
            .date-dropdown .Select-value,
            .date-dropdown .Select-placeholder {
                line-height: 38px !important;
                padding-left: 12px !important;
                color: #1f2937 !important;
                font-weight: 500 !important;
            }
            
            .date-dropdown .Select-placeholder {
                color: #9ca3af !important;
            }
            
            .date-dropdown .Select-arrow {
                border-color: #6b7280 transparent transparent !important;
                border-width: 6px 5px 3px !important;
            }
            
            .date-dropdown .Select-menu-outer {
                border: 2px solid #e0e0e0 !important;
                border-radius: 8px !important;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15) !important;
                margin-top: 4px !important;
            }
            
            .date-dropdown .VirtualizedSelectOption {
                padding: 10px 12px !important;
                font-size: 14px !important;
                transition: background-color 0.15s ease !important;
            }
            
            .date-dropdown .VirtualizedSelectOption:hover {
                background-color: #eff6ff !important;
                color: #1e40af !important;
            }
            
            .date-dropdown .VirtualizedSelectFocusedOption {
                background-color: #dbeafe !important;
                color: #1e40af !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

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
available_years = sorted(list(set([date.year for date in all_dates])))
available_months_by_year = {}
for year in available_years:
    available_months_by_year[year] = sorted(list(set([date.month for date in all_dates if date.year == year])))

# Default start: first available month/year
start_year_default = all_dates[0].year
start_month_default = all_dates[0].month

# Default end: last available month/year
end_year_default = all_dates[-1].year
end_month_default = all_dates[-1].month

# Create the layout
app.layout = html.Div(
    [
        # Header Section
        html.Div(
            [
                html.Div(
                    [
                        html.H1(
                            "Arrearage Counts Dashboard",
                            style={
                                "color": "white",
                                "margin": 0,
                                "fontSize": "32px",
                                "fontWeight": "600",
                            },
                        ),
                        html.P(
                            "Track utility arrearage trends across the Northwest",
                            style={
                                "color": "rgba(255, 255, 255, 0.9)",
                                "margin": "8px 0 0 0",
                                "fontSize": "16px",
                            },
                        ),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Div(
                            id="last-updated",
                            children=f"Last Updated: {datetime.now().strftime('%B %d, %Y')}",
                            style={
                                "color": "rgba(255, 255, 255, 0.8)",
                                "fontSize": "14px",
                                "textAlign": "right",
                            },
                        ),
                    ],
                ),
            ],
            style={
                "background": "linear-gradient(135deg, #156570 0%, #0d4b52 100%)",
                "padding": "30px 40px",
                "marginBottom": "30px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
            },
        ),
        # KPI Cards Section
        html.Div(
            id="kpi-cards",
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))",
                "gap": "20px",
                "marginBottom": "30px",
            },
        ),
        # Filters Section
        html.Div(
            [
                html.H3(
                    "Filters",
                    style={
                        "marginTop": 0,
                        "marginBottom": "20px",
                        "color": "#2c3e50",
                        "fontSize": "20px",
                        "fontWeight": "600",
                    },
                ),
                html.Div(
                    [
                        html.Label(
                            "Select Date Range:",
                            style={"fontWeight": "600", "color": "#2c3e50", "marginBottom": "15px", "display": "block"},
                        ),
                        html.Div(
                            [
                                # Start date selectors
                                html.Div(
                                    [
                                        html.Label(
                                            "Start Date",
                                            style={
                                                "fontSize": "14px",
                                                "color": "#2c3e50",
                                                "marginBottom": "10px",
                                                "display": "block",
                                                "fontWeight": "600",
                                            },
                                        ),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        dcc.Dropdown(
                                                            id="start-month-picker",
                                                            options=[
                                                                {"label": month_names[i - 1], "value": i}
                                                                for i in available_months_by_year[start_year_default]
                                                            ],
                                                            value=start_month_default,
                                                            clearable=False,
                                                            searchable=False,
                                                            className="date-dropdown",
                                                        ),
                                                    ],
                                                    style={"flex": "1", "marginRight": "10px"},
                                                ),
                                                html.Div(
                                                    [
                                                        dcc.Dropdown(
                                                            id="start-year-picker",
                                                            options=[
                                                                {"label": str(year), "value": year}
                                                                for year in available_years
                                                            ],
                                                            value=start_year_default,
                                                            clearable=False,
                                                            searchable=False,
                                                            className="date-dropdown",
                                                        ),
                                                    ],
                                                    style={"flex": "0 0 100px"},
                                                ),
                                            ],
                                            style={"display": "flex", "gap": "10px"},
                                        ),
                                    ],
                                    style={
                                        "flex": "0 0 320px",
                                        "padding": "15px",
                                        "backgroundColor": "#f8f9fa",
                                        "borderRadius": "8px",
                                        "border": "1px solid #e0e0e0",
                                    },
                                ),
                                # End date selectors
                                html.Div(
                                    [
                                        html.Label(
                                            "End Date",
                                            style={
                                                "fontSize": "14px",
                                                "color": "#2c3e50",
                                                "marginBottom": "10px",
                                                "display": "block",
                                                "fontWeight": "600",
                                            },
                                        ),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        dcc.Dropdown(
                                                            id="end-month-picker",
                                                            options=[
                                                                {"label": month_names[i - 1], "value": i}
                                                                for i in available_months_by_year[end_year_default]
                                                            ],
                                                            value=end_month_default,
                                                            clearable=False,
                                                            searchable=False,
                                                            className="date-dropdown",
                                                        ),
                                                    ],
                                                    style={"flex": "1", "marginRight": "10px"},
                                                ),
                                                html.Div(
                                                    [
                                                        dcc.Dropdown(
                                                            id="end-year-picker",
                                                            options=[
                                                                {"label": str(year), "value": year}
                                                                for year in available_years
                                                            ],
                                                            value=end_year_default,
                                                            clearable=False,
                                                            searchable=False,
                                                            className="date-dropdown",
                                                        ),
                                                    ],
                                                    style={"flex": "0 0 100px"},
                                                ),
                                            ],
                                            style={"display": "flex", "gap": "10px"},
                                        ),
                                    ],
                                    style={
                                        "flex": "0 0 320px",
                                        "padding": "15px",
                                        "backgroundColor": "#f8f9fa",
                                        "borderRadius": "8px",
                                        "border": "1px solid #e0e0e0",
                                    },
                                ),
                            ],
                            style={"display": "flex", "gap": "20px"},
                        ),
                    ],
                    style={"marginBottom": 25},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Select Utilities:",
                                    style={
                                        "fontWeight": "600",
                                        "color": "#2c3e50",
                                        "marginBottom": "10px",
                                        "display": "block",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Button(
                                            "Select All",
                                            id="select-all-btn",
                                            n_clicks=0,
                                            style={
                                                "marginRight": "10px",
                                                "padding": "6px 16px",
                                                "backgroundColor": "#156570",
                                                "color": "white",
                                                "border": "none",
                                                "borderRadius": "4px",
                                                "cursor": "pointer",
                                                "fontSize": "14px",
                                                "fontWeight": "500",
                                            },
                                        ),
                                        html.Button(
                                            "Clear All",
                                            id="clear-all-btn",
                                            n_clicks=0,
                                            style={
                                                "padding": "6px 16px",
                                                "backgroundColor": "#95a5a6",
                                                "color": "white",
                                                "border": "none",
                                                "borderRadius": "4px",
                                                "cursor": "pointer",
                                                "fontSize": "14px",
                                                "fontWeight": "500",
                                            },
                                        ),
                                    ],
                                    style={"marginBottom": "15px"},
                                ),
                            ],
                        ),
                        # Utility chips
                        html.Div(
                            id="utility-chips-container",
                            children=[
                                html.Button(
                                    util,
                                    id={"type": "utility-chip", "index": util},
                                    n_clicks=0,
                                    style={
                                        "padding": "10px 20px",
                                        "margin": "5px",
                                        "backgroundColor": "#156570",
                                        "color": "white",
                                        "border": "2px solid #156570",
                                        "borderRadius": "25px",
                                        "cursor": "pointer",
                                        "fontSize": "14px",
                                        "fontWeight": "500",
                                        "transition": "all 0.3s ease",
                                        "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                    },
                                )
                                for util in all_utilities
                            ],
                            style={
                                "display": "flex",
                                "flexWrap": "wrap",
                                "gap": "5px",
                            },
                        ),
                        # Hidden storage for selected utilities
                        dcc.Store(id="selected-utilities-store", data=all_utilities),
                    ],
                ),
            ],
            style={
                "padding": "25px",
                "backgroundColor": "white",
                "borderRadius": "8px",
                "marginBottom": "30px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                "border": "1px solid #e1e8ed",
            },
        ),
        # Tabs for Chart and Table
        dcc.Tabs(
            id="tabs",
            value="chart-tab",
            children=[
                dcc.Tab(
                    label="📊 Stacked Area Chart",
                    value="chart-tab",
                    style={
                        "padding": "12px 24px",
                        "fontWeight": "500",
                        "fontSize": "15px",
                    },
                    selected_style={
                        "padding": "12px 24px",
                        "fontWeight": "600",
                        "fontSize": "15px",
                        "borderTop": "3px solid #156570",
                        "backgroundColor": "white",
                    },
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H3(
                                            "Total Arrearage Counts by Utility",
                                            style={
                                                "marginTop": 0,
                                                "marginBottom": "5px",
                                                "color": "#2c3e50",
                                                "fontSize": "22px",
                                                "fontWeight": "600",
                                            },
                                        ),
                                        html.P(
                                            id="chart-subtitle",
                                            style={
                                                "color": "#7f8c8d",
                                                "fontSize": "14px",
                                                "margin": "0 0 20px 0",
                                            },
                                        ),
                                    ],
                                ),
                                dcc.Loading(
                                    id="loading-chart",
                                    type="default",
                                    color="#156570",
                                    children=html.Div(
                                        [
                                            dcc.Graph(id="stacked-area-chart", config={"displayModeBar": True}),
                                        ]
                                    ),
                                ),
                            ],
                            style={
                                "padding": "25px",
                                "backgroundColor": "white",
                                "borderRadius": "8px",
                                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                                "border": "1px solid #e1e8ed",
                            },
                        )
                    ],
                ),
                dcc.Tab(
                    label="📋 Data Table",
                    value="table-tab",
                    style={
                        "padding": "12px 24px",
                        "fontWeight": "500",
                        "fontSize": "15px",
                    },
                    selected_style={
                        "padding": "12px 24px",
                        "fontWeight": "600",
                        "fontSize": "15px",
                        "borderTop": "3px solid #156570",
                        "backgroundColor": "white",
                    },
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H3(
                                            "Arrearage Count Data",
                                            style={
                                                "marginTop": 0,
                                                "marginBottom": "5px",
                                                "color": "#2c3e50",
                                                "fontSize": "22px",
                                                "fontWeight": "600",
                                                "display": "inline-block",
                                            },
                                        ),
                                        html.Button(
                                            "⬇ Download CSV",
                                            id="download-btn",
                                            n_clicks=0,
                                            style={
                                                "float": "right",
                                                "padding": "8px 20px",
                                                "backgroundColor": "#156570",
                                                "color": "white",
                                                "border": "none",
                                                "borderRadius": "4px",
                                                "cursor": "pointer",
                                                "fontSize": "14px",
                                                "fontWeight": "500",
                                            },
                                        ),
                                        dcc.Download(id="download-dataframe-csv"),
                                    ],
                                    style={"marginBottom": "20px", "overflow": "auto"},
                                ),
                                dcc.Loading(
                                    id="loading-table",
                                    type="default",
                                    color="#156570",
                                    children=html.Div(
                                        [
                                            dash_table.DataTable(
                                                id="data-table",
                                                columns=[
                                                    {"name": "Utility", "id": "Utility"},
                                                    {"name": "Zip Code", "id": "Zip Code"},
                                                    {"name": "Month", "id": "Month"},
                                                    {
                                                        "name": "Arrearage Count",
                                                        "id": "Arrearage Count",
                                                        "type": "numeric",
                                                        "format": {"specifier": ",.0f"},
                                                    },
                                                ],
                                                style_table={"overflowX": "auto"},
                                                style_cell={
                                                    "textAlign": "left",
                                                    "padding": "12px",
                                                    "fontSize": "14px",
                                                    "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                                                },
                                                style_header={
                                                    "backgroundColor": "#156570",
                                                    "color": "white",
                                                    "fontWeight": "600",
                                                    "fontSize": "14px",
                                                    "textAlign": "left",
                                                    "padding": "14px",
                                                },
                                                style_data_conditional=[
                                                    {
                                                        "if": {"row_index": "odd"},
                                                        "backgroundColor": "#f8f9fa",
                                                    },
                                                    {
                                                        "if": {"state": "selected"},
                                                        "backgroundColor": "#e8f4f5",
                                                        "border": "1px solid #156570",
                                                    },
                                                ],
                                                page_size=25,
                                                sort_action="native",
                                                filter_action="native",
                                                style_filter={
                                                    "backgroundColor": "#f1f3f5",
                                                    "fontWeight": "normal",
                                                },
                                            ),
                                        ]
                                    ),
                                ),
                            ],
                            style={
                                "padding": "25px",
                                "backgroundColor": "white",
                                "borderRadius": "8px",
                                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                                "border": "1px solid #e1e8ed",
                            },
                        )
                    ],
                ),
            ],
            style={"marginBottom": "30px"},
        ),
        # Footer
        html.Div(
            [
                html.Div(
                    [
                        html.P(
                            [
                                "Data Source: UTC Docket Case 200281 | ",
                                html.Span(
                                    f"Dashboard Generated: {datetime.now().strftime('%B %d, %Y')}",
                                    style={"fontWeight": "500"},
                                ),
                            ],
                            style={"margin": 0, "fontSize": "14px", "color": "#7f8c8d"},
                        ),
                    ],
                    style={"textAlign": "center"},
                ),
            ],
            style={
                "padding": "20px",
                "backgroundColor": "#f8f9fa",
                "borderTop": "1px solid #e1e8ed",
                "marginTop": "30px",
            },
        ),
    ],
    style={
        "padding": "0",
        "maxWidth": "1600px",
        "margin": "0 auto",
        "backgroundColor": "#f5f7fa",
        "minHeight": "100vh",
        "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
    },
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
def update_utility_selection(_chip_clicks, _select_all, _clear_all, current_selection):
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


def create_chips(selected_utilities):
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
def update_start_month_options(selected_year):
    """Update available months based on selected year for start date."""
    if selected_year in available_months_by_year:
        return [{"label": month_names[i - 1], "value": i} for i in available_months_by_year[selected_year]]
    return [{"label": month_names[i - 1], "value": i + 1} for i in range(12)]


# Callback to update month options when year changes for end date
@app.callback(
    Output("end-month-picker", "options"),
    Input("end-year-picker", "value"),
)
def update_end_month_options(selected_year):
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
def update_dashboard(start_month, start_year, end_month, end_year, selected_utilities):
    """Update the KPI cards, chart and table based on filter selections."""
    # Convert month/year to datetime objects
    start_date = datetime(start_year, start_month, 1) if start_month and start_year else all_dates[0]
    end_date = datetime(end_year, end_month, 1) if end_month and end_year else all_dates[-1]

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
        filtered_df = filtered_df.filter(pl.lit(False))

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

    # Create KPI cards
    kpi_cards = [
        html.Div(
            [
                html.Div(
                    "📈 Total Arrearages",
                    style={"fontSize": "14px", "color": "#7f8c8d", "marginBottom": "8px", "fontWeight": "500"},
                ),
                html.Div(
                    f"{total_arrearages:,.0f}",
                    style={"fontSize": "28px", "fontWeight": "700", "color": "#2c3e50", "marginBottom": "4px"},
                ),
                html.Div(
                    f"{percent_change:+.1f}% from first to last month" if len(monthly_totals) >= 2 else "N/A",
                    style={
                        "fontSize": "12px",
                        "color": "#27ae60" if percent_change < 0 else "#e74c3c" if percent_change > 0 else "#7f8c8d",
                        "fontWeight": "500",
                    },
                ),
            ],
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                "border": "1px solid #e1e8ed",
            },
        ),
        html.Div(
            [
                html.Div(
                    "📊 Avg. Monthly Count",
                    style={"fontSize": "14px", "color": "#7f8c8d", "marginBottom": "8px", "fontWeight": "500"},
                ),
                html.Div(
                    f"{avg_monthly:,.0f}",
                    style={"fontSize": "28px", "fontWeight": "700", "color": "#2c3e50", "marginBottom": "4px"},
                ),
                html.Div(
                    f"Across {len(monthly_totals)} months",
                    style={"fontSize": "12px", "color": "#7f8c8d", "fontWeight": "500"},
                ),
            ],
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                "border": "1px solid #e1e8ed",
            },
        ),
        html.Div(
            [
                html.Div(
                    "📍 Unique Zip Codes",
                    style={"fontSize": "14px", "color": "#7f8c8d", "marginBottom": "8px", "fontWeight": "500"},
                ),
                html.Div(
                    f"{unique_zips:,}",
                    style={"fontSize": "28px", "fontWeight": "700", "color": "#2c3e50", "marginBottom": "4px"},
                ),
                html.Div(
                    f"In {num_utilities} utilities",
                    style={"fontSize": "12px", "color": "#7f8c8d", "fontWeight": "500"},
                ),
            ],
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                "border": "1px solid #e1e8ed",
            },
        ),
        html.Div(
            [
                html.Div(
                    "🏢 Active Utilities",
                    style={"fontSize": "14px", "color": "#7f8c8d", "marginBottom": "8px", "fontWeight": "500"},
                ),
                html.Div(
                    f"{num_utilities}",
                    style={"fontSize": "28px", "fontWeight": "700", "color": "#2c3e50", "marginBottom": "4px"},
                ),
                html.Div(
                    f"of {len(all_utilities)} total",
                    style={"fontSize": "12px", "color": "#7f8c8d", "fontWeight": "500"},
                ),
            ],
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                "border": "1px solid #e1e8ed",
            },
        ),
    ]

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
def download_csv(n_clicks, start_month, start_year, end_month, end_year, selected_utilities):
    """Download the filtered data as CSV."""
    if n_clicks is None or n_clicks == 0:
        return None

    # Convert month/year to datetime objects
    start_date = datetime(start_year, start_month, 1) if start_month and start_year else all_dates[0]
    end_date = datetime(end_year, end_month, 1) if end_month and end_year else all_dates[-1]

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
        filtered_df = filtered_df.filter(pl.lit(False))

    # Convert to pandas and prepare for download
    table_df = filtered_df.to_pandas()
    table_df["Month"] = table_df["Month"].dt.strftime("%b %Y")
    csv_string = table_df.to_csv(index=False)

    return {"content": csv_string, "filename": "arrearage_counts_export.csv"}


if __name__ == "__main__":
    # Bind only to localhost by default for improved security; set BIND_HOST to override if needed.
    bind_host = os.environ.get("BIND_HOST", "localhost")
    port = int(os.environ.get("PORT", "8080"))

    # Set debug mode based on STAGE environment variable
    # debug=False in production, debug=True for development/local
    stage = os.environ.get("STAGE", "dev").lower()
    debug_mode = stage != "prod"

    app.run(debug=debug_mode, host=bind_host, port=port)
