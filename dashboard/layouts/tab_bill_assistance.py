"""Tab 3: Bill Assistance - comprehensive visualizations."""

from dash import dash_table, dcc, html


def create_bill_assistance_tab() -> dcc.Tab:
    """Create comprehensive Bill Assistance tab with all required visualizations."""
    # Common card style for all visualization containers
    card_style = {
        "padding": "25px",
        "backgroundColor": "white",
        "borderRadius": "8px",
        "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
        "border": "1px solid #e1e8ed",
        "marginBottom": "25px",
    }

    # Common title style
    title_style = {
        "marginTop": 0,
        "marginBottom": "5px",
        "color": "#2c3e50",
        "fontSize": "22px",
        "fontWeight": "600",
    }

    # Common subtitle style
    subtitle_style = {
        "color": "#7f8c8d",
        "fontSize": "14px",
        "margin": "0 0 20px 0",
    }

    return dcc.Tab(
        label="🤝 Bill Assistance",
        value="bill-assistance-tab",
        style={
            "padding": "12px 30px",
            "fontWeight": "500",
            "fontSize": "16px",
        },
        selected_style={
            "padding": "12px 30px",
            "fontWeight": "700",
            "fontSize": "16px",
            "borderTop": "4px solid #156570",
            "backgroundColor": "white",
        },
        children=[
            html.Div(
                [
                    # Section 1: Bill Assistance Enrollment
                    html.H2(
                        "Bill Assistance Enrollment",
                        style={
                            "color": "#2c3e50",
                            "fontSize": "26px",
                            "fontWeight": "700",
                            "marginTop": "20px",
                            "marginBottom": "20px",
                            "paddingBottom": "10px",
                            "borderBottom": "2px solid #156570",
                        },
                    ),
                    # Chart 1: Stacked line graph - bill assistance by utility
                    html.Div(
                        [
                            html.H3(
                                "Number of Customers Enrolled in a Bill Assistance Program by Utility",
                                style=title_style,
                            ),
                            html.P(
                                id="ba-enrollment-by-utility-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="ba-enrollment-by-utility-stacked-line",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 2: Individual utility trendlines (5 graphs in grid)
                    html.Div(
                        [
                            html.H3(
                                "Bill Assistance Enrollment - Individual Utility Trends",
                                style=title_style,
                            ),
                            html.P(
                                id="ba-enrollment-individual-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="ba-enrollment-individual-trendlines",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Section 2: Payment Arrangements
                    html.H2(
                        "Payment Arrangements",
                        style={
                            "color": "#2c3e50",
                            "fontSize": "26px",
                            "fontWeight": "700",
                            "marginTop": "40px",
                            "marginBottom": "20px",
                            "paddingBottom": "10px",
                            "borderBottom": "2px solid #156570",
                        },
                    ),
                    # Chart 3: Stacked line graph - payment arrangements by utility
                    html.Div(
                        [
                            html.H3(
                                "Number of Customers with Payment Arrangements by Utility",
                                style=title_style,
                            ),
                            html.P(
                                id="ba-payment-arr-by-utility-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="ba-payment-arr-by-utility-stacked-line",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 4: Individual utility trendlines for payment arrangements
                    html.Div(
                        [
                            html.H3(
                                "Payment Arrangements - Individual Utility Trends",
                                style=title_style,
                            ),
                            html.P(
                                id="ba-payment-arr-individual-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="ba-payment-arr-individual-trendlines",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Data Table Section
                    html.H2(
                        "Data Tables",
                        style={
                            "color": "#2c3e50",
                            "fontSize": "26px",
                            "fontWeight": "700",
                            "marginTop": "40px",
                            "marginBottom": "20px",
                            "paddingBottom": "10px",
                            "borderBottom": "2px solid #156570",
                        },
                    ),
                    # Data table tabs
                    dcc.Tabs(
                        id="ba-data-tables",
                        value="ba-enrollment-table",
                        children=[
                            dcc.Tab(
                                label="📋 Bill Assistance Enrollment",
                                value="ba-enrollment-table",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.H3(
                                                        "Bill Assistance Enrollment Data",
                                                        style={
                                                            **title_style,
                                                            "display": "inline-block",
                                                        },
                                                    ),
                                                    html.Button(
                                                        "⬇ Download CSV",
                                                        id="download-ba-enrollment-btn",
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
                                                    dcc.Download(id="download-ba-enrollment-csv"),
                                                ],
                                                style={"marginBottom": "20px", "overflow": "auto"},
                                            ),
                                            dcc.Loading(
                                                type="default",
                                                color="#156570",
                                                children=dash_table.DataTable(
                                                    id="ba-enrollment-data-table",
                                                    columns=[
                                                        {"name": "Utility", "id": "Utility"},
                                                        {"name": "Zip Code", "id": "Zip Code"},
                                                        {"name": "Month", "id": "Month"},
                                                        {
                                                            "name": "Bill Assist Customer Count",
                                                            "id": "Bill Assist Customer Count",
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
                                            ),
                                        ],
                                        style=card_style,
                                    ),
                                ],
                            ),
                            dcc.Tab(
                                label="📋 Payment Arrangements",
                                value="ba-payment-arr-table",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.H3(
                                                        "Payment Arrangements Data",
                                                        style={
                                                            **title_style,
                                                            "display": "inline-block",
                                                        },
                                                    ),
                                                    html.Button(
                                                        "⬇ Download CSV",
                                                        id="download-ba-payment-arr-btn",
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
                                                    dcc.Download(id="download-ba-payment-arr-csv"),
                                                ],
                                                style={"marginBottom": "20px", "overflow": "auto"},
                                            ),
                                            dcc.Loading(
                                                type="default",
                                                color="#156570",
                                                children=dash_table.DataTable(
                                                    id="ba-payment-arr-data-table",
                                                    columns=[
                                                        {"name": "Utility", "id": "Utility"},
                                                        {"name": "Zip Code", "id": "Zip Code"},
                                                        {"name": "Month", "id": "Month"},
                                                        {
                                                            "name": "Payment Agreement Customer Count",
                                                            "id": "Payment Agreement Customer Count",
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
                                            ),
                                        ],
                                        style=card_style,
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
                style={"padding": "20px"},
            )
        ],
    )
