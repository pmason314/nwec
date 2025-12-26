"""Callbacks for Past-Due Balances - Customer Counts visualizations."""

from datetime import UTC, datetime

import numpy as np
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from plotly.subplots import make_subplots
from scipy import stats


def register_counts_callbacks(
    app: Dash,
    counts_data: pl.DataFrame,
    all_utilities: list[str],
    colors: dict[str, str],
) -> None:
    """Register callbacks for customer counts visualizations.

    Args:
        app: Dash app instance
        counts_data: Arrearage counts dataset
        all_utilities: List of all utility names
        colors: Utility color mapping
    """

    # ========================================
    # Chart 1.1: Stacked line - all utilities counts
    # ========================================
    @app.callback(
        [
            Output("pdb-counts-all-subtitle", "children"),
            Output("pdb-counts-stacked-line", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_counts_stacked_line(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create stacked line chart for customer counts by utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = counts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = (
            filtered_df.group_by(["Date", "Utility"])
            .agg(pl.col("Arrearage Customer Count").sum())
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
                        y=utility_data["Arrearage Customer Count"],
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

        subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # ========================================
    # Chart 1.2: Individual utility trendlines
    # ========================================
    @app.callback(
        [
            Output("pdb-counts-individual-subtitle", "children"),
            Output("pdb-counts-individual-trendlines", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_counts_individual_trendlines(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create individual trendline charts for each utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = counts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = (
            filtered_df.group_by(["Date", "Utility"])
            .agg(pl.col("Arrearage Customer Count").sum())
            .sort("Date")
            .to_pandas()
        )

        # Create subplots
        utilities_to_show = selected_utilities if selected_utilities else all_utilities
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
                        y=utility_data["Arrearage Customer Count"],
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
                y_values = utility_data["Arrearage Customer Count"].to_numpy()
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
                title_text="Customers" if col == 1 else "",
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
        )

        subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # ========================================
    # Data table callback for counts
    # ========================================
    @app.callback(
        Output("pdb-counts-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_counts_table(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> list[dict]:
        """Update counts data table."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = counts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

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
        table_df = table_df[["Utility", "Zip Code", "Month", "Arrearage Customer Count"]]
        return table_df.to_dict("records")

    # ========================================
    # CSV download callback for counts
    # ========================================
    @app.callback(
        Output("download-pdb-counts-csv", "data"),
        Input("download-pdb-counts-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_counts_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download counts data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = counts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Arrearage Customer Count"]]
        csv_string = table_df.to_csv(index=False)

        return {"content": csv_string, "filename": "past_due_balances_counts_export.csv"}
