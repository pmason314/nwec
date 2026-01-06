"""Callbacks for disconnection notices visualizations."""

from datetime import UTC, datetime

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output


def register_disconnection_notices_callbacks(
    app: Dash,
    disconnection_notices_data: pl.DataFrame,
    all_utilities: list[str],
    colors: dict[str, str],
) -> None:
    """Register callbacks for disconnection notices visualizations.

    Args:
        app: Dash app instance
        disconnection_notices_data: Disconnection notices dataset
        all_utilities: List of all utility names
        colors: Utility color mapping
    """

    # Chart 3: Stacked line - disconnection notices by utility
    @app.callback(
        [
            Output("disc-notices-by-utility-subtitle", "children"),
            Output("disc-notices-by-utility-stacked-line", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_notices_by_utility(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create stacked line chart for disconnection notices by utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = disconnection_notices_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = (
            filtered_df.group_by(["Date", "Utility"])
            .agg(pl.col("Disconnection Notice Count").sum())
            .sort("Date")
            .to_pandas()
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities if selected_utilities else all_utilities:
            utility_data = chart_data[chart_data["Utility"] == utility].sort_values("Date")
            if not utility_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Date"],
                        y=utility_data["Disconnection Notice Count"],
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
            yaxis_title="Number of Customers",
            legend={
                "title": {"text": "Utility", "font": {"size": 14}},
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
            },
            yaxis={
                "showgrid": True,
                "gridcolor": "#e1e8ed",
                "tickformat": ",.0f",
            },
        )

        subtitle = f"Showing data from {start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # Data table callback
    @app.callback(
        Output("disc-notices-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_notices_table(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> list[dict]:
        """Update disconnection notices data table."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = disconnection_notices_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Sort by Date for chronological order before formatting
        filtered_df = filtered_df.sort(["Utility", "Date", "Zip Code"])
        table_df = filtered_df.to_pandas()
        # Format Month as YYYY-MM for sortable display
        table_df["Month"] = table_df["Date"].dt.strftime("%Y-%m")
        # Drop Date column, only keep Month
        table_df = table_df[["Utility", "Zip Code", "Month", "Disconnection Notice Count"]]
        return table_df.to_dict("records")

    # CSV download callback
    @app.callback(
        Output("download-disc-notices-csv", "data"),
        Input("download-disc-notices-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_disc_notices_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download disconnection notices data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = disconnection_notices_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Disconnection Notice Count"]]
        csv_string = table_df.to_csv(index=False)

        return {"content": csv_string, "filename": "disconnection_notices_export.csv"}
