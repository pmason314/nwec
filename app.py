"""Dash web dashboard for arrearage counts visualization."""

from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output, dash_table, dcc, html

# Load data
DATA_PATH = Path(__file__).parent / "data" / "utility_reporting" / "processed" / "arrearage_counts.arrow"
arrearage_counts = pl.read_ipc(DATA_PATH)

# Initialize the Dash app
app = Dash(__name__)
server = app.server  # Expose the server for deployment

# Get unique values for filters
all_utilities = sorted(arrearage_counts["Utility"].unique().to_list())
all_dates = sorted(arrearage_counts["Month"].unique().to_list())

# Create the layout
app.layout = html.Div(
    [
        html.H1("Arrearage Counts Dashboard", style={"textAlign": "center", "marginBottom": 30}),
        # Filters Section
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Select Date Range:", style={"fontWeight": "bold"}),
                        dcc.RangeSlider(
                            id="date-range-slider",
                            min=0,
                            max=len(all_dates) - 1,
                            value=[0, len(all_dates) - 1],
                            marks={i: date.strftime("%b-%y") for i, date in enumerate(all_dates)},
                            step=1,
                        ),
                    ],
                    style={"marginBottom": 20},
                ),
                html.Div(
                    [
                        html.Label("Select Utilities:", style={"fontWeight": "bold"}),
                        dcc.Checklist(
                            id="utility-checklist",
                            options=[{"label": util, "value": util} for util in all_utilities],
                            value=all_utilities,  # All selected by default
                            inline=True,
                            style={"display": "flex", "gap": "20px"},
                        ),
                    ],
                    style={"marginBottom": 30},
                ),
            ],
            style={"padding": "20px", "backgroundColor": "#f8f9fa", "borderRadius": "5px", "marginBottom": 30},
        ),
        # Tabs for Chart and Table
        dcc.Tabs(
            id="tabs",
            value="chart-tab",
            children=[
                dcc.Tab(
                    label="Stacked Area Chart",
                    value="chart-tab",
                    children=[
                        html.Div(
                            [
                                html.H3("Total Arrearage Counts by Utility", style={"marginTop": 20}),
                                dcc.Graph(id="stacked-area-chart"),
                            ],
                        )
                    ],
                ),
                dcc.Tab(
                    label="Data Table",
                    value="table-tab",
                    children=[
                        html.Div(
                            [
                                html.H3("Arrearage Count Data", style={"marginTop": 20}),
                                dash_table.DataTable(
                                    id="data-table",
                                    columns=[
                                        {"name": "Utility", "id": "Utility"},
                                        {"name": "Zip Code", "id": "Zip Code"},
                                        {"name": "Month", "id": "Month"},
                                        {"name": "Arrearage Count", "id": "Arrearage Count"},
                                    ],
                                    style_table={"overflowX": "auto"},
                                    style_cell={"textAlign": "left", "padding": "10px"},
                                    style_header={"backgroundColor": "#156570", "color": "white", "fontWeight": "bold"},
                                    style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#f8f9fa"}],
                                    page_size=20,
                                    sort_action="native",
                                    filter_action="native",
                                ),
                            ],
                            style={"padding": "20px"},
                        )
                    ],
                ),
            ],
        ),
    ],
    style={"padding": "20px", "maxWidth": "1400px", "margin": "0 auto"},
)


# Callback to update chart and table
@app.callback(
    [Output("stacked-area-chart", "figure"), Output("data-table", "data")],
    [Input("date-range-slider", "value"), Input("utility-checklist", "value")],
)
def update_dashboard(date_range, selected_utilities):
    """Update the chart and table based on filter selections."""
    # Filter data based on selections
    start_date_idx, end_date_idx = date_range
    start_date = all_dates[start_date_idx]
    end_date = all_dates[end_date_idx]

    filtered_df = arrearage_counts.filter(
        (pl.col("Month") >= start_date)
        & (pl.col("Month") <= end_date)
        & (pl.col("Utility").is_in(selected_utilities if selected_utilities else all_utilities))
    )

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
                    line=dict(width=0.5, color=colors.get(utility, "#cccccc")),
                    hovertemplate=f"<b>{utility}</b><br>%{{y:,.0f}}<extra></extra>",
                )
            )

    fig.update_layout(
        xaxis_title="",
        yaxis_title="Arrearage Count",
        legend_title="Utility",
        hovermode="x unified",
        plot_bgcolor="white",
        height=500,
        xaxis=dict(tickformat="%b-%y", showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )

    # Prepare table data
    table_df = filtered_df.to_pandas()
    table_df["Month"] = table_df["Month"].dt.strftime("%Y-%m-%d")
    table_data = table_df.to_dict("records")

    return fig, table_data


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
