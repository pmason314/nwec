"""Callbacks for Past-Due Balances - Vintage Analysis visualizations."""

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from scipy import stats


def register_vintage_callbacks(
    app: Dash,
    amounts_data: pl.DataFrame,
    vintage_colors: dict[str, str],
) -> None:
    """Register callbacks for vintage analysis visualizations.

    Args:
        app: Dash app instance
        amounts_data: Arrearage amounts dataset
        vintage_colors: Vintage color mapping
    """

    # ========================================
    # Chart 3.1: Stacked bar by vintage with trendline
    # ========================================
    @app.callback(
        [
            Output("pdb-vintage-stacked-subtitle", "children"),
            Output("pdb-vintage-stacked-bar", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_vintage_stacked_bar(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create stacked bar chart by vintage with trendline."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = (
            amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))
            .filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))
            .filter(
                pl.col("Vintage") != "Total Arrearages"  # Exclude total
            )
        )

        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate by date and vintage
        chart_data = (
            filtered_df.group_by(["Date", "Vintage"]).agg(pl.col("Arrearage_Amount").sum()).sort("Date").to_pandas()
        )

        # Create figure
        fig = go.Figure()

        # Define vintage order
        vintage_order = ["30 Days", "60 Days", "90 Days +"]

        for vintage in vintage_order:
            vintage_data = chart_data[chart_data["Vintage"] == vintage].sort_values("Date")
            if not vintage_data.empty:
                fig.add_trace(
                    go.Bar(
                        x=vintage_data["Date"],
                        y=vintage_data["Arrearage_Amount"],
                        name=vintage,
                        marker_color=vintage_colors.get(vintage, "#cccccc"),
                        hovertemplate=f"<b>{vintage}</b><br>${{y:,.2f}}<extra></extra>",
                    )
                )

        # Calculate total for trendline
        total_by_date = chart_data.groupby("Date")["Arrearage_Amount"].sum().reset_index()

        if len(total_by_date) > 1:
            x_numeric = np.arange(len(total_by_date))
            y_values = total_by_date["Arrearage_Amount"].to_numpy()
            slope, intercept, _, _, _ = stats.linregress(x_numeric, y_values)
            trendline = slope * x_numeric + intercept

            fig.add_trace(
                go.Scatter(
                    x=total_by_date["Date"],
                    y=trendline,
                    name="Trend",
                    mode="lines",
                    line={"color": "#e74c3c", "width": 3, "dash": "dash"},
                    hovertemplate="<b>Trendline</b><br>$%{y:,.2f}<extra></extra>",
                )
            )

        fig.update_layout(
            barmode="stack",
            xaxis_title="",
            yaxis_title="Past-Due Balance ($USD)",
            legend={
                "title": {"text": "Days Past Due", "font": {"size": 14}},
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
                "tickformat": "$,.0f",
            },
        )

        subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"
        return subtitle, fig

    # ========================================
    # Chart 3.2: Pie chart - most recent full year
    # ========================================
    @app.callback(
        [
            Output("pdb-vintage-pie-subtitle", "children"),
            Output("pdb-vintage-pie", "figure"),
        ],
        [
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_vintage_pie(selected_utilities: list[str]) -> tuple[str, go.Figure]:
        """Create pie chart for most recent full year by vintage."""
        if not selected_utilities:
            selected_utilities = []

        # Get most recent full year
        max_year = amounts_data["Year"].max()

        # Filter for most recent full year
        year_data = amounts_data.filter(pl.col("Year") == max_year).filter(pl.col("Vintage") != "Total Arrearages")

        if selected_utilities:
            year_data = year_data.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            year_data = year_data.filter(pl.lit(value=False))

        # Aggregate by vintage
        chart_data = (
            year_data.group_by("Vintage")
            .agg(pl.col("Arrearage_Amount").sum())
            .sort("Arrearage_Amount", descending=True)
            .to_pandas()
        )

        # Define vintage order for pie
        vintage_order = ["30 Days", "60 Days", "90 Days +"]
        chart_data["Vintage"] = pd.Categorical(chart_data["Vintage"], categories=vintage_order, ordered=True)
        chart_data = chart_data.sort_values("Vintage")

        # Create pie chart
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=chart_data["Vintage"],
                    values=chart_data["Arrearage_Amount"],
                    marker={"colors": [vintage_colors.get(v, "#cccccc") for v in chart_data["Vintage"]]},
                    textposition="auto",
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>$%{value:,.2f}<br>%{percent}<extra></extra>",
                )
            ]
        )

        fig.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            height=550,
            margin={"l": 60, "r": 60, "t": 20, "b": 60},
            showlegend=True,
            legend={
                "orientation": "v",
                "yanchor": "top",
                "y": 1,
                "xanchor": "left",
                "x": 1.02,
            },
        )

        subtitle = f"Data for full year {max_year}"
        return subtitle, fig
