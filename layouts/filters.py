"""Filters section with date range and utility selection."""

from dash import dcc, html


def create_filters(
    all_utilities: list[str],
    available_months_by_year: dict[int, list[int]],
    available_years: list[int],
    start_year_default: int,
    start_month_default: int,
    end_year_default: int,
    end_month_default: int,
    month_names: list[str],
) -> html.Div:
    """Create the filters section with date range and utility chips."""
    return html.Div(
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
    )
