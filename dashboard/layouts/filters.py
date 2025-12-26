"""Filters section with date range and utility selection."""

from dash import dcc, html

from dashboard.dashboard_config import UTILITY_COLORS, UTILITY_DISPLAY_NAMES


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
    # Sort utilities by display name length (shortest first)
    sorted_utilities = sorted(
        all_utilities,
        key=lambda u: len(UTILITY_DISPLAY_NAMES.get(u, u)),
    )

    return html.Div(
        [
            html.H3(
                "Filters",
                style={
                    "marginTop": 0,
                    "marginBottom": "20px",
                    "color": "#2c3e50",
                    "fontSize": "24px",
                    "fontWeight": "700",
                },
            ),
            # Two-column layout: Date range on left, Utility selection on right
            html.Div(
                [
                    # Left column: Date Range (in card)
                    html.Div(
                        [
                            html.Label(
                                "Select Date Range:",
                                style={
                                    "fontWeight": "600",
                                    "color": "#2c3e50",
                                    "marginBottom": "20px",
                                    "display": "block",
                                    "fontSize": "18px",
                                },
                            ),
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
                                    "marginBottom": "20px",
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
                            ),
                        ],
                        style={
                            "flex": "0 0 auto",
                            "minWidth": "380px",
                            "padding": "25px",
                            "backgroundColor": "#f8f9fa",
                            "borderRadius": "8px",
                            "border": "1px solid #e0e0e0",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.05)",
                        },
                    ),
                    # Subtle divider
                    html.Div(
                        style={
                            "width": "1px",
                            "backgroundColor": "#d0d0d0",
                            "alignSelf": "stretch",
                        }
                    ),
                    # Right column: Utility Selection (in card)
                    html.Div(
                        [
                            html.Label(
                                "Select Utilities:",
                                style={
                                    "fontWeight": "600",
                                    "color": "#2c3e50",
                                    "marginBottom": "15px",
                                    "display": "block",
                                    "fontSize": "18px",
                                    "textAlign": "center",
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
                                            "padding": "10px 24px",
                                            "backgroundColor": "#156570",
                                            "color": "white",
                                            "border": "none",
                                            "borderRadius": "6px",
                                            "cursor": "pointer",
                                            "fontSize": "15px",
                                            "fontWeight": "600",
                                            "boxShadow": "0 2px 6px rgba(21, 101, 112, 0.3)",
                                            "transition": "all 0.2s ease",
                                        },
                                    ),
                                    html.Button(
                                        "Clear All",
                                        id="clear-all-btn",
                                        n_clicks=0,
                                        style={
                                            "padding": "10px 24px",
                                            "backgroundColor": "#95a5a6",
                                            "color": "white",
                                            "border": "none",
                                            "borderRadius": "6px",
                                            "cursor": "pointer",
                                            "fontSize": "15px",
                                            "fontWeight": "600",
                                            "boxShadow": "0 2px 6px rgba(149, 165, 166, 0.3)",
                                            "transition": "all 0.2s ease",
                                        },
                                    ),
                                ],
                                style={
                                    "marginBottom": "20px",
                                    "display": "flex",
                                    "justifyContent": "center",
                                },
                            ),
                            # Utility chips
                            html.Div(
                                id="utility-chips-container",
                                children=[
                                    html.Button(
                                        UTILITY_DISPLAY_NAMES.get(util, util),
                                        id={"type": "utility-chip", "index": util},
                                        n_clicks=0,
                                        style={
                                            "padding": "10px 20px",
                                            "backgroundColor": UTILITY_COLORS.get(util, "#003768"),
                                            "color": "white",
                                            "border": f"2px solid {UTILITY_COLORS.get(util, '#003768')}",
                                            "borderRadius": "25px",
                                            "cursor": "pointer",
                                            "fontSize": "14px",
                                            "fontWeight": "500",
                                            "transition": "all 0.3s ease",
                                            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                            "display": "inline-block",
                                        },
                                    )
                                    for util in sorted_utilities
                                ],
                                style={
                                    "display": "grid",
                                    "gridTemplateColumns": "repeat(3, auto)",
                                    "gap": "10px",
                                    "justifyContent": "center",
                                },
                            ),
                            # Hidden storage for selected utilities
                            dcc.Store(id="selected-utilities-store", data=all_utilities),
                        ],
                        style={
                            "flex": "0 0 auto",
                            "minWidth": "380px",
                            "padding": "25px",
                            "backgroundColor": "#f8f9fa",
                            "borderRadius": "8px",
                            "border": "1px solid #e0e0e0",
                            "boxShadow": "0 1px 3px rgba(0,0,0,0.05)",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "stretch",
                    "justifyContent": "center",
                    "gap": "30px",
                },
            ),
        ],
        style={
            "padding": "30px 50px",
            "backgroundColor": "white",
            "borderRadius": "8px",
            "marginBottom": "30px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
            "border": "1px solid #e1e8ed",
            "maxWidth": "1400px",
            "marginLeft": "auto",
            "marginRight": "auto",
        },
    )
