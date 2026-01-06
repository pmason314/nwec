"""Tab 1: Past-Due Balances - comprehensive visualizations."""

from dash import dash_table, dcc, html


def create_past_due_balances_tab() -> dcc.Tab:
    """Create comprehensive Past-Due Balances tab with all required visualizations."""
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
        label="📈 Past-Due Balances",
        value="past-due-balances-tab",
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
                    # Section 1: Number of Customers with Past-Due Balances
                    html.H2(
                        "Number of Customers with Past-Due Balances",
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
                    # Chart 1.1: Stacked line graph - all utilities
                    html.Div(
                        [
                            html.H3(
                                "Number of Customers with Past-Due Balances by Utility",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-counts-all-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-counts-stacked-line",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 1.2: Individual utility trendlines (5 graphs in grid)
                    html.Div(
                        [
                            html.H3(
                                "Number of Customers with Past-Due Balances - Individual Utility Trends",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-counts-individual-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-counts-individual-trendlines",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Section 2: Total Amount of Past-Due Balances
                    html.H2(
                        "Total Amount of Past-Due Balances",
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
                    # Chart 2.1: Stacked line graph - total amounts
                    html.Div(
                        [
                            html.H3(
                                "Total Amount of Past-Due Balances",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-amounts-total-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-amounts-stacked-line",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 2.2: March comparison - total balances
                    html.Div(
                        [
                            html.H3(
                                "Past-Due Balances by Utility in March of Each Year",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-amounts-march-total-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-amounts-march-total",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 2.3: March comparison - average balances
                    html.Div(
                        [
                            html.H3(
                                "Average Past-Due Balance by Utility in March of Each Year",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-amounts-march-avg-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-amounts-march-avg",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Section 3: Past-Due Balances by Vintage (Days Past Due)
                    html.H2(
                        "Past-Due Balances by Days Past Due",
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
                    # Chart 3.1: Stacked bar chart by vintage with trendline
                    html.Div(
                        [
                            html.H3(
                                "Past-Due Balances by Days Past Due",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-vintage-stacked-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-vintage-stacked-bar",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 3.2: Pie chart - most recent full year
                    html.Div(
                        [
                            html.H3(
                                "Percentage of Past-Due Balances by Days Past Due",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-vintage-pie-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-vintage-pie",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Section 4: Low-Income Past-Due Balances (KLI)
                    html.H2(
                        "Low-Income Customer Past-Due Balances",
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
                    # Chart 4.1: KLI stacked bar chart by vintage
                    html.Div(
                        [
                            html.H3(
                                "Low-Income Past-Due Balances by Days Past Due",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-kli-vintage-stacked-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-kli-vintage-stacked-bar",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 4.2: KLI pie chart - most recent full year
                    html.Div(
                        [
                            html.H3(
                                "Percentage of Low-Income Past-Due Balances by Days Past Due",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-kli-vintage-pie-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-kli-vintage-pie",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Chart 4.3: KLI clustered bar chart for March
                    html.Div(
                        [
                            html.H3(
                                "Low-Income Past-Due Balances by Days Past Due in March",
                                style=title_style,
                            ),
                            html.P(
                                id="pdb-kli-march-clustered-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="pdb-kli-march-clustered-bar",
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
                        id="pdb-data-tables",
                        value="pdb-counts-table",
                        children=[
                            dcc.Tab(
                                label="📋 Past-Due Balance Counts",
                                value="pdb-counts-table",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.H3(
                                                        "Past-Due Balance Count Data",
                                                        style={
                                                            **title_style,
                                                            "display": "inline-block",
                                                        },
                                                    ),
                                                    html.Button(
                                                        "⬇ Download CSV",
                                                        id="download-pdb-counts-btn",
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
                                                    dcc.Download(id="download-pdb-counts-csv"),
                                                ],
                                                style={"marginBottom": "20px", "overflow": "auto"},
                                            ),
                                            dcc.Loading(
                                                type="default",
                                                color="#156570",
                                                children=dash_table.DataTable(
                                                    id="pdb-counts-data-table",
                                                    columns=[
                                                        {"name": "Utility", "id": "Utility"},
                                                        {"name": "Zip Code", "id": "Zip Code"},
                                                        {"name": "Month", "id": "Month"},
                                                        {
                                                            "name": "Arrearage Customer Count",
                                                            "id": "Arrearage Customer Count",
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
                                label="💰 Past-Due Balance Amounts",
                                value="pdb-amounts-table",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.H3(
                                                        "Past-Due Balance Amount Data",
                                                        style={
                                                            **title_style,
                                                            "display": "inline-block",
                                                        },
                                                    ),
                                                    html.Button(
                                                        "⬇ Download CSV",
                                                        id="download-pdb-amounts-btn",
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
                                                    dcc.Download(id="download-pdb-amounts-csv"),
                                                ],
                                                style={"marginBottom": "20px", "overflow": "auto"},
                                            ),
                                            dcc.Loading(
                                                type="default",
                                                color="#156570",
                                                children=dash_table.DataTable(
                                                    id="pdb-amounts-data-table",
                                                    columns=[
                                                        {"name": "Utility", "id": "Utility"},
                                                        {"name": "Zip Code", "id": "Zip Code"},
                                                        {"name": "Month", "id": "Month"},
                                                        {"name": "Vintage", "id": "Vintage"},
                                                        {
                                                            "name": "Arrearage Amount",
                                                            "id": "Arrearage_Amount",
                                                            "type": "numeric",
                                                            "format": {"specifier": "$,.2f"},
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
                                label="🏠 KLI Past-Due Balance Amounts",
                                value="pdb-kli-table",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                [
                                                    html.H3(
                                                        "Low-Income Past-Due Balance Amount Data",
                                                        style={
                                                            **title_style,
                                                            "display": "inline-block",
                                                        },
                                                    ),
                                                    html.Button(
                                                        "⬇ Download CSV",
                                                        id="download-pdb-kli-btn",
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
                                                    dcc.Download(id="download-pdb-kli-csv"),
                                                ],
                                                style={"marginBottom": "20px", "overflow": "auto"},
                                            ),
                                            dcc.Loading(
                                                type="default",
                                                color="#156570",
                                                children=dash_table.DataTable(
                                                    id="pdb-kli-data-table",
                                                    columns=[
                                                        {"name": "Utility", "id": "Utility"},
                                                        {"name": "Zip Code", "id": "Zip Code"},
                                                        {"name": "Month", "id": "Month"},
                                                        {"name": "Vintage", "id": "Vintage"},
                                                        {
                                                            "name": "Arrearage Amount",
                                                            "id": "Arrearage_Amount",
                                                            "type": "numeric",
                                                            "format": {"specifier": "$,.2f"},
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
