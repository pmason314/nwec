"""Callbacks for Past-Due Balances - Total Amounts visualizations."""

from datetime import UTC, datetime

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output

from dashboard.chart_builders import create_stacked_line_chart


def register_amounts_callbacks(
    app: Dash,
    amounts_data: pl.DataFrame,
    counts_data: pl.DataFrame,
    all_utilities: list[str],
    colors: dict[str, str],
) -> None:
    """Register callbacks for amounts visualizations.

    Args:
        app: Dash app instance
        amounts_data: Arrearage amounts dataset
        counts_data: Arrearage counts dataset (for average calculations)
        all_utilities: List of all utility names
        colors: Utility color mapping
    """

    # ========================================
    # Chart 2.1: Stacked line - total amounts
    # ========================================
    @app.callback(
        [
            Output("pdb-amounts-total-subtitle", "children"),
            Output("pdb-amounts-stacked-line", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_amounts_stacked_line(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create stacked line chart for total arrearage amounts by utility."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and utility
        chart_data = filtered_df.group_by(["Date", "Utility"]).agg(pl.col("Arrearage_Amount").sum()).sort("Date")

        # Create figure using centralized chart builder
        utilities_to_show = selected_utilities if selected_utilities else all_utilities
        fig = create_stacked_line_chart(
            data=chart_data,
            utilities=utilities_to_show,
            value_column="Arrearage_Amount",
            y_axis_title="Past-Due Balances ($USD)",
            utility_colors=colors,
            is_amount=True,
            height=550,
        )

        subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # ========================================
    # Chart 2.2: March comparison - total balances
    # ========================================
    @app.callback(
        [
            Output("pdb-amounts-march-total-subtitle", "children"),
            Output("pdb-amounts-march-total", "figure"),
        ],
        [
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_amounts_march_total(selected_utilities: list[str]) -> tuple[str, go.Figure]:
        """Create line chart comparing March balances across years."""
        if not selected_utilities:
            selected_utilities = []

        # Filter for March data only
        march_data = amounts_data.filter(pl.col("Month") == 3)

        if selected_utilities:
            march_data = march_data.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            march_data = march_data.filter(pl.lit(value=False))

        # Aggregate by year and utility
        chart_data = (
            march_data.group_by(["Year", "Utility"]).agg(pl.col("Arrearage_Amount").sum()).sort("Year").to_pandas()
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities if selected_utilities else all_utilities:
            utility_data = chart_data[chart_data["Utility"] == utility].sort_values("Year")
            if not utility_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Year"],
                        y=utility_data["Arrearage_Amount"],
                        name=utility,
                        mode="lines+markers",
                        line={"color": colors.get(utility, "#cccccc"), "width": 2},
                        marker={"size": 8},
                        hovertemplate=f"<b>{utility}</b><br>Year: %{{x}}<br>${{y:,.2f}}<extra></extra>",
                    )
                )

        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Past-Due Balances ($USD)",
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
                "showgrid": True,
                "gridcolor": "#e1e8ed",
                "dtick": 1,
            },
            yaxis={
                "showgrid": True,
                "gridcolor": "#e1e8ed",
                "tickformat": "$,.0f",
            },
        )

        subtitle = "Comparing March data across all available years"
        return subtitle, fig

    # ========================================
    # Chart 2.3: March comparison - average balances
    # ========================================
    @app.callback(
        [
            Output("pdb-amounts-march-avg-subtitle", "children"),
            Output("pdb-amounts-march-avg", "figure"),
        ],
        [
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_amounts_march_avg(selected_utilities: list[str]) -> tuple[str, go.Figure]:
        """Create line chart comparing average March balances across years."""
        if not selected_utilities:
            selected_utilities = []

        # Filter for March data only
        march_amounts = amounts_data.filter(pl.col("Month") == 3)
        march_counts = counts_data.filter(pl.col("Month") == 3)

        if selected_utilities:
            march_amounts = march_amounts.filter(pl.col("Utility").is_in(selected_utilities))
            march_counts = march_counts.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            march_amounts = march_amounts.filter(pl.lit(value=False))
            march_counts = march_counts.filter(pl.lit(value=False))

        # Calculate totals
        amounts_by_year = (
            march_amounts.group_by(["Year", "Utility"])
            .agg(pl.col("Arrearage_Amount").sum().alias("Total_Amount"))
            .sort("Year")
        )

        counts_by_year = (
            march_counts.group_by(["Year", "Utility"])
            .agg(pl.col("Arrearage Customer Count").sum().alias("Total_Count"))
            .sort("Year")
        )

        # Join and calculate average
        chart_data = (
            amounts_by_year.join(counts_by_year, on=["Year", "Utility"], how="inner")
            .with_columns((pl.col("Total_Amount") / pl.col("Total_Count")).alias("Avg_Balance"))
            .to_pandas()
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities if selected_utilities else all_utilities:
            utility_data = chart_data[chart_data["Utility"] == utility].sort_values("Year")
            if not utility_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Year"],
                        y=utility_data["Avg_Balance"],
                        name=utility,
                        mode="lines+markers",
                        line={"color": colors.get(utility, "#cccccc"), "width": 2},
                        marker={"size": 8},
                        hovertemplate=f"<b>{utility}</b><br>Year: %{{x}}<br>${{y:,.2f}}<extra></extra>",
                    )
                )

        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Average Past-Due Balance ($USD)",
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
                "showgrid": True,
                "gridcolor": "#e1e8ed",
                "dtick": 1,
            },
            yaxis={
                "showgrid": True,
                "gridcolor": "#e1e8ed",
                "tickformat": "$,.2f",
            },
        )

        subtitle = "Comparing average March balances across all available years"
        return subtitle, fig

    # ========================================
    # Data table callback for amounts
    # ========================================
    @app.callback(
        Output("pdb-amounts-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_amounts_table(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> list[dict]:
        """Update amounts data table."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Sort by Date for chronological order before formatting
        filtered_df = filtered_df.sort(["Utility", "Date", "Zip Code", "Vintage"])
        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Vintage", "Arrearage_Amount"]]
        return table_df.to_dict("records")

    # ========================================
    # CSV download callback for amounts
    # ========================================
    @app.callback(
        Output("download-pdb-amounts-csv", "data"),
        Input("download-pdb-amounts-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_amounts_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download amounts data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Vintage", "Arrearage_Amount"]]
        csv_string = table_df.to_csv(index=False)

        return {"content": csv_string, "filename": "past_due_balances_amounts_export.csv"}
