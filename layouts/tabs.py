"""Tabs section with chart and data table."""

from dash import dash_table, dcc, html


def create_tabs() -> dcc.Tabs:
    """Create tabs for chart and table views."""
    return dcc.Tabs(
        id="tabs",
        value="chart-tab",
        children=[
            dcc.Tab(
                label="📊 Arrearage County by Utility",
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
                                                "fontFamily": (
                                                    "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
                                                ),
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
    )
