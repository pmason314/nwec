"""Dynamic tab generation for datasets."""

from dash import dash_table, dcc, html


def create_dataset_subtabs(
    dataset_id: str, dataset_name: str, value_column: str, is_amount: bool = False, has_vintage: bool = False
) -> dcc.Tabs:
    """Create sub-tabs for a dataset (Chart and Data Table).

    Args:
        dataset_id: Unique identifier for the dataset (used in component IDs)
        dataset_name: Display name for the dataset
        value_column: Name of the value column in the data
        is_amount: Whether the value is a dollar amount (True) or count (False)
        has_vintage: Whether the dataset has a Vintage column
    """
    # Format specifier for the value column
    value_format = {"specifier": "$,.2f"} if is_amount else {"specifier": ",.0f"}

    # Build table columns dynamically
    table_columns = [
        {"name": "Utility", "id": "Utility"},
        {"name": "Zip Code", "id": "Zip Code"},
        {"name": "Month", "id": "Month"},
    ]
    if has_vintage:
        table_columns.append({"name": "Vintage", "id": "Vintage"})
    table_columns.append(
        {
            "name": value_column,
            "id": value_column,
            "type": "numeric",
            "format": value_format,
        }
    )

    return dcc.Tabs(
        id=f"{dataset_id}-subtabs",
        value=f"{dataset_id}-chart-tab",
        children=[
            dcc.Tab(
                label="Chart View",
                value=f"{dataset_id}-chart-tab",
                style={
                    "padding": "8px 20px",
                    "fontWeight": "500",
                    "fontSize": "14px",
                },
                selected_style={
                    "padding": "8px 20px",
                    "fontWeight": "600",
                    "fontSize": "14px",
                    "borderTop": "2px solid #156570",
                    "backgroundColor": "white",
                },
                children=[
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        f"Total {dataset_name} by Utility",
                                        style={
                                            "marginTop": 0,
                                            "marginBottom": "5px",
                                            "color": "#2c3e50",
                                            "fontSize": "22px",
                                            "fontWeight": "600",
                                        },
                                    ),
                                    html.P(
                                        id=f"{dataset_id}-chart-subtitle",
                                        style={
                                            "color": "#7f8c8d",
                                            "fontSize": "14px",
                                            "margin": "0 0 20px 0",
                                        },
                                    ),
                                ],
                            ),
                            dcc.Loading(
                                id=f"loading-{dataset_id}-chart",
                                type="default",
                                color="#156570",
                                children=html.Div(
                                    [
                                        dcc.Graph(id=f"{dataset_id}-chart", config={"displayModeBar": True}),
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
                label="Data Table",
                value=f"{dataset_id}-table-tab",
                style={
                    "padding": "8px 20px",
                    "fontWeight": "500",
                    "fontSize": "14px",
                },
                selected_style={
                    "padding": "8px 20px",
                    "fontWeight": "600",
                    "fontSize": "14px",
                    "borderTop": "2px solid #156570",
                    "backgroundColor": "white",
                },
                children=[
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        f"{dataset_name} Data",
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
                                        id=f"download-{dataset_id}-btn",
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
                                    dcc.Download(id=f"download-{dataset_id}-csv"),
                                ],
                                style={"marginBottom": "20px", "overflow": "auto"},
                            ),
                            dcc.Loading(
                                id=f"loading-{dataset_id}-table",
                                type="default",
                                color="#156570",
                                children=html.Div(
                                    [
                                        dash_table.DataTable(
                                            id=f"{dataset_id}-table",
                                            columns=table_columns,
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
    )


def create_dynamic_tabs(dataset_configs: list) -> dcc.Tabs:
    """Create main tabs dynamically from dataset configurations.

    Args:
        dataset_configs: List of DatasetConfig objects
    """
    tab_children = []

    for config in dataset_configs:
        dataset_id = config.file_name.replace("_", "-")

        # Check if dataset has Vintage column
        has_vintage = "amount" in config.file_name.lower()

        tab_children.append(
            dcc.Tab(
                label=config.name,
                value=f"{dataset_id}-tab",
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
                    create_dataset_subtabs(dataset_id, config.name, config.value_column, config.is_amount, has_vintage)
                ],
            )
        )

    return dcc.Tabs(
        id="main-tabs",
        value=f"{dataset_configs[0].file_name.replace('_', '-')}-tab" if dataset_configs else "tab1",
        children=tab_children,
        style={"marginBottom": "30px"},
    )
