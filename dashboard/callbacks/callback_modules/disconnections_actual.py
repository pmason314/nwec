"""Callbacks for actual disconnections visualizations."""

from datetime import UTC, datetime

import numpy as np
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from plotly.subplots import make_subplots
from scipy import stats

from dashboard.chart_builders import create_stacked_line_chart


def register_actual_disconnections_callbacks(
    app: Dash,
    disconnections_data: pl.DataFrame,
    all_utilities: list[str],
    colors: dict[str, str],
) -> None:
    """Register callbacks for actual disconnections visualizations.

    Args:
        app: Dash app instance
        disconnections_data: Disconnections dataset
        all_utilities: List of all utility names
        colors: Utility color mapping
    """

    # Chart 1: Stacked line - disconnections by utility
    @app.callback(
        [
            Output("disc-by-utility-subtitle", "children"),
            Output("disc-by-utility-stacked-line", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_by_utility(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create stacked line chart for disconnections by utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = disconnections_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = filtered_df.group_by(["Date", "Utility"]).agg(pl.col("Number of Disconnects").sum()).sort("Date")

        # Create figure using centralized chart builder
        utilities_to_show = selected_utilities or all_utilities
        fig = create_stacked_line_chart(
            data=chart_data,
            utilities=utilities_to_show,
            value_column="Number of Disconnects",
            y_axis_title="Number of Disconnected Customers",
            utility_colors=colors,
            is_amount=False,
            height=550,
            title="Number of Disconnections by Utility",
        )

        subtitle = f"Showing data from {start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # Chart 1.2: Individual utility trendlines
    @app.callback(
        [
            Output("disc-individual-subtitle", "children"),
            Output("disc-individual-trendlines", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_individual_trendlines(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create individual trendline charts for each utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Return empty figure if no utilities selected
        if len(selected_utilities) == 0:
            empty_fig = go.Figure()
            empty_fig.update_layout(
                xaxis={"visible": False},
                yaxis={"visible": False},
                annotations=[
                    {
                        "text": "No utilities selected. Please select at least one utility to view trends.",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 14, "color": "#7f8c8d"},
                        "x": 0.5,
                        "y": 0.5,
                        "xanchor": "center",
                        "yanchor": "middle",
                    }
                ],
                plot_bgcolor="white",
                paper_bgcolor="white",
                height=400,
            )
            return "No utilities selected", empty_fig

        # Filter data
        filtered_df = disconnections_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = (
            filtered_df.group_by(["Date", "Utility"])
            .agg(pl.col("Number of Disconnects").sum())
            .sort("Date")
            .to_pandas()
        )

        # Create subplots
        utilities_to_show = selected_utilities or all_utilities
        n_utilities = len(utilities_to_show)

        # Arrange in grid (2 or 3 columns)
        n_cols = min(3, n_utilities) if n_utilities > 2 else n_utilities
        n_rows = int(np.ceil(n_utilities / n_cols))

        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            subplot_titles=[f"{u}" for u in utilities_to_show],
            vertical_spacing=0.12,
            horizontal_spacing=0.10,
        )

        for idx, utility in enumerate(utilities_to_show):
            row = idx // n_cols + 1
            col = idx % n_cols + 1

            utility_data = chart_data[chart_data["Utility"] == utility].sort_values("Date")

            if not utility_data.empty:
                # Add actual data line
                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Date"],
                        y=utility_data["Number of Disconnects"],
                        name=utility,
                        mode="lines",
                        line={"color": colors.get(utility, "#cccccc"), "width": 2},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )

                # Calculate and add trendline
                x_numeric = np.arange(len(utility_data))
                y_values = utility_data["Number of Disconnects"].to_numpy()
                slope, intercept, _, _, _ = stats.linregress(x_numeric, y_values)
                trendline = slope * x_numeric + intercept

                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Date"],
                        y=trendline,
                        name=f"{utility} Trend",
                        mode="lines",
                        line={"color": colors.get(utility, "#cccccc"), "width": 1, "dash": "dash"},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )

            # Update axis labels
            fig.update_xaxes(
                tickformat="%b-%y",
                showgrid=True,
                gridcolor="#e1e8ed",
                row=row,
                col=col,
            )
            fig.update_yaxes(
                title_text="Disconnections" if col == 1 else "",
                showgrid=True,
                gridcolor="#e1e8ed",
                tickformat=",.0f",
                row=row,
                col=col,
            )

        fig.update_layout(
            height=350 * n_rows,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin={"l": 60, "r": 40, "t": 60, "b": 40},
            title={
                "text": "Disconnections - Individual Utility Trends",
                "x": 0.5,
                "xanchor": "center",
                "font": {"size": 18, "color": "#2c3e50"},
            },
        )

        subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # Chart 2: Total disconnections with trendline
    @app.callback(
        [
            Output("disc-total-trendline-subtitle", "children"),
            Output("disc-total-trendline", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_total_trendline(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create line chart with trendline for total disconnections."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = disconnections_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date
        chart_data = filtered_df.group_by("Date").agg(pl.col("Number of Disconnects").sum()).sort("Date").to_pandas()

        # Create figure
        fig = go.Figure()

        if not chart_data.empty:
            # Add actual data line
            fig.add_trace(
                go.Scatter(
                    x=chart_data["Date"],
                    y=chart_data["Number of Disconnects"],
                    name="Total Disconnections",
                    mode="lines",
                    line={"color": "#156570", "width": 2},
                    hovertemplate="<b>Total</b><br>%{y:,.0f}<extra></extra>",
                )
            )

            # Calculate and add trendline
            if len(chart_data) > 1:
                x_numeric = np.arange(len(chart_data))
                y_values = chart_data["Number of Disconnects"].values
                slope, intercept, _, _, _ = stats.linregress(x_numeric, y_values)
                trendline = slope * x_numeric + intercept

                fig.add_trace(
                    go.Scatter(
                        x=chart_data["Date"],
                        y=trendline,
                        name="Trend",
                        mode="lines",
                        line={"color": "#e74c3c", "width": 2, "dash": "dash"},
                        hovertemplate="<b>Trendline</b><br>%{y:,.0f}<extra></extra>",
                    )
                )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Number of Disconnected Customers",
            legend={
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
            margin={"l": 60, "r": 140, "t": 60, "b": 60},
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
            title={
                "text": "Total Number of Disconnections",
                "x": 0.5,
                "xanchor": "center",
                "font": {"size": 18, "color": "#2c3e50"},
            },
        )

        subtitle = f"Showing data from {start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # Data table callback
    @app.callback(
        Output("disc-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_disc_table(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> list[dict]:
        """Update disconnections data table."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = disconnections_data.with_columns(
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
        table_df = table_df[["Utility", "Zip Code", "Month", "Number of Disconnects"]]
        return table_df.to_dict("records")

    # CSV download callback
    @app.callback(
        Output("download-disc-csv", "data"),
        Input("download-disc-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_disc_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download disconnections data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = disconnections_data.with_columns(
            pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")
        ).filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Number of Disconnects"]]
        csv_string = table_df.to_csv(index=False)

        return {"content": csv_string, "filename": "disconnections_export.csv"}
