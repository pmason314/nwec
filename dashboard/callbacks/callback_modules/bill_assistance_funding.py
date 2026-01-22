"""Callbacks for bill assistance funding (LIHEAP and utility programs)."""

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from plotly.subplots import make_subplots


def register_funding_callbacks(
    app: Dash,
    df_liheap: pl.DataFrame,
    df_utility: pl.DataFrame,
    utility_colors: dict[str, str],
) -> None:
    """Register callbacks for bill assistance funding visualizations.

    Args:
        app: Dash app instance
        df_liheap: DataFrame with LIHEAP assistance data
        df_utility: DataFrame with utility assistance program data
        utility_colors: Dictionary mapping utility names to colors
    """

    @app.callback(
        [
            Output("ba-liheap-dollars-stacked-bar", "figure"),
            Output("ba-liheap-dollars-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_liheap_dollars_chart(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> tuple:
        """Update LIHEAP assistance dollars stacked bar chart."""
        # Filter data by date range and utilities
        filtered_df = df_liheap.filter(
            (pl.col("Year") > start_year) | ((pl.col("Year") == start_year) & (pl.col("Month") >= start_month))
        ).filter((pl.col("Year") < end_year) | ((pl.col("Year") == end_year) & (pl.col("Month") <= end_month)))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))

        if len(filtered_df) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="No data available for selected filters",
                xaxis={"visible": False},
                yaxis={"visible": False},
            )
            return fig, "No data available"

        # Group by year-month and utility
        grouped_df = (
            filtered_df.group_by(["Year", "Month", "Utility"])
            .agg(pl.col("Assistance Amount").sum())
            .sort(["Year", "Month"])
        )

        # Create date column for x-axis
        grouped_df = grouped_df.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))

        # Create stacked bar chart
        fig = go.Figure()

        utilities = sorted(grouped_df["Utility"].unique().to_list())
        for utility in utilities:
            utility_data = grouped_df.filter(pl.col("Utility") == utility)
            fig.add_trace(
                go.Bar(
                    x=utility_data["Date"].to_list(),
                    y=utility_data["Assistance Amount"].to_list(),
                    name=utility,
                    marker_color=utility_colors.get(utility, "#156570"),
                    hovertemplate=f"<b>{utility}</b><br>"
                    + "Date: %{x|%b %Y}<br>"
                    + "Amount: $%{y:,.0f}<extra></extra>",
                )
            )

        fig.update_layout(
            barmode="stack",
            xaxis_title="Date",
            yaxis_title="LIHEAP Assistance ($USD)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
            height=500,
        )

        total_amount = grouped_df["Assistance Amount"].sum()
        subtitle = f"Total LIHEAP assistance: ${total_amount:,.0f}"

        return fig, subtitle

    @app.callback(
        [
            Output("ba-utility-dollars-stacked-bar", "figure"),
            Output("ba-utility-dollars-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_utility_dollars_chart(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> tuple:
        """Update utility assistance dollars stacked bar chart."""
        # Filter data by date range and utilities
        filtered_df = df_utility.filter(
            (pl.col("Year") > start_year) | ((pl.col("Year") == start_year) & (pl.col("Month") >= start_month))
        ).filter((pl.col("Year") < end_year) | ((pl.col("Year") == end_year) & (pl.col("Month") <= end_month)))

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))

        if len(filtered_df) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="No data available for selected filters",
                xaxis={"visible": False},
                yaxis={"visible": False},
            )
            return fig, "No data available"

        # Group by year-month and utility
        grouped_df = (
            filtered_df.group_by(["Year", "Month", "Utility"])
            .agg(pl.col("Assistance Amount").sum())
            .sort(["Year", "Month"])
        )

        # Create date column for x-axis
        grouped_df = grouped_df.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))

        # Create stacked bar chart
        fig = go.Figure()

        utilities = sorted(grouped_df["Utility"].unique().to_list())
        for utility in utilities:
            utility_data = grouped_df.filter(pl.col("Utility") == utility)
            fig.add_trace(
                go.Bar(
                    x=utility_data["Date"].to_list(),
                    y=utility_data["Assistance Amount"].to_list(),
                    name=utility,
                    marker_color=utility_colors.get(utility, "#156570"),
                    hovertemplate=f"<b>{utility}</b><br>"
                    + "Date: %{x|%b %Y}<br>"
                    + "Amount: $%{y:,.0f}<extra></extra>",
                )
            )

        fig.update_layout(
            barmode="stack",
            xaxis_title="Date",
            yaxis_title="Utility Assistance ($USD)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
            height=500,
        )

        total_amount = grouped_df["Assistance Amount"].sum()
        subtitle = f"Total utility assistance: ${total_amount:,.0f}"

        return fig, subtitle
