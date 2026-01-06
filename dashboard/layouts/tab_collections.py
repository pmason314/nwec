"""Tab 4: Collections - collection agency referrals visualization."""

from dash import dash_table, dcc, html


def create_collections_tab() -> dcc.Tab:
    """Create Collections tab with collection agency referrals visualization."""
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
        label="📞 Collections",
        value="collections-tab",
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
                    # Section: Collection Agency Referrals
                    html.H2(
                        "Collection Agency Referrals",
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
                    # Chart: Stacked line graph - collection agency referrals by utility
                    html.Div(
                        [
                            html.H3(
                                "Number of Customers Referred to Collection Agencies by Utility",
                                style=title_style,
                            ),
                            html.P(
                                id="collections-referrals-by-utility-subtitle",
                                style=subtitle_style,
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dcc.Graph(
                                    id="collections-referrals-by-utility-stacked-line",
                                    config={"displayModeBar": True},
                                ),
                            ),
                        ],
                        style=card_style,
                    ),
                    # Data Table Section
                    html.H2(
                        "Data Table",
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
                    # Data table for collection agency referrals
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        "Collection Agency Referrals Data",
                                        style={
                                            **title_style,
                                            "display": "inline-block",
                                        },
                                    ),
                                    html.Button(
                                        "⬇ Download CSV",
                                        id="download-collections-referrals-btn",
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
                                    dcc.Download(id="download-collections-referrals-csv"),
                                ],
                                style={"marginBottom": "20px", "overflow": "auto"},
                            ),
                            dcc.Loading(
                                type="default",
                                color="#156570",
                                children=dash_table.DataTable(
                                    id="collections-referrals-data-table",
                                    columns=[
                                        {"name": "Utility", "id": "Utility"},
                                        {"name": "Zip Code", "id": "Zip Code"},
                                        {"name": "Month", "id": "Month"},
                                        {
                                            "name": "Collections Accounts Customer Count",
                                            "id": "Collections Accounts Customer Count",
                                            "type": "numeric",
                                            "format": {"specifier": ",.0f"},
                                        },
                                    ],
                                    style_table={"overflowX": "auto"},
                                    style_cell={
                                        "textAlign": "left",
                                        "padding": "12px",
                                        "fontSize": "14px",
                                        "fontFamily": ("-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
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
                style={"padding": "20px"},
            )
        ],
    )
