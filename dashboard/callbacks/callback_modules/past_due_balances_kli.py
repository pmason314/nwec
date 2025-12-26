"""Callbacks for Past-Due Balances - KLI (Known Low-Income) visualizations."""

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from scipy import stats


def register_kli_callbacks(
    app: Dash,
    kli_amounts_data: pl.DataFrame,
    vintage_colors: dict[str, str],
) -> None:
    """Register callbacks for KLI visualizations.

    Args:
        app: Dash app instance
        kli_amounts_data: KLI arrearage amounts dataset
        vintage_colors: Vintage color mapping
    """

    # ========================================
    # Chart 4.1: KLI stacked bar by vintage
    # ========================================
    @app.callback(
        [
            Output("pdb-kli-vintage-stacked-subtitle", "children"),
            Output("pdb-kli-vintage-stacked-bar", "figure"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_kli_vintage_stacked_bar(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure]:
        """Create KLI stacked bar chart by vintage with trendline."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        # Filter data
        filtered_df = (
            kli_amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))
            .filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))
            .filter(pl.col("Vintage") != "Total Arrearages")
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
    # Chart 4.2: KLI pie chart - most recent full year
    # ========================================
    @app.callback(
        [
            Output("pdb-kli-vintage-pie-subtitle", "children"),
            Output("pdb-kli-vintage-pie", "figure"),
        ],
        [
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_kli_vintage_pie(selected_utilities: list[str]) -> tuple[str, go.Figure]:
        """Create KLI pie chart for most recent full year by vintage."""
        if not selected_utilities:
            selected_utilities = []

        # Get most recent full year
        max_year = kli_amounts_data["Year"].max()

        # Filter for most recent full year
        year_data = kli_amounts_data.filter(pl.col("Year") == max_year).filter(pl.col("Vintage") != "Total Arrearages")

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

    # ========================================
    # Chart 4.3: KLI clustered bar for March
    # ========================================
    @app.callback(
        [
            Output("pdb-kli-march-clustered-subtitle", "children"),
            Output("pdb-kli-march-clustered-bar", "figure"),
        ],
        [
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_kli_march_clustered(selected_utilities: list[str]) -> tuple[str, go.Figure]:
        """Create KLI clustered bar chart for March data by vintage."""
        if not selected_utilities:
            selected_utilities = []

        # Filter for March data only
        march_data = kli_amounts_data.filter(pl.col("Month") == 3).filter(pl.col("Vintage") != "Total Arrearages")

        if selected_utilities:
            march_data = march_data.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            march_data = march_data.filter(pl.lit(value=False))

        # Aggregate by year and vintage
        chart_data = (
            march_data.group_by(["Year", "Vintage"]).agg(pl.col("Arrearage_Amount").sum()).sort("Year").to_pandas()
        )

        # Create figure
        fig = go.Figure()

        vintage_order = ["30 Days", "60 Days", "90 Days +"]

        for vintage in vintage_order:
            vintage_data = chart_data[chart_data["Vintage"] == vintage].sort_values("Year")
            if not vintage_data.empty:
                fig.add_trace(
                    go.Bar(
                        x=vintage_data["Year"],
                        y=vintage_data["Arrearage_Amount"],
                        name=vintage,
                        marker_color=vintage_colors.get(vintage, "#cccccc"),
                        hovertemplate=f"<b>{vintage}</b><br>Year: %{{x}}<br>${{y:,.2f}}<extra></extra>",
                    )
                )

        fig.update_layout(
            barmode="group",
            xaxis_title="Year",
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
    # Data table callback for KLI
    # ========================================
    @app.callback(
        Output("pdb-kli-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_kli_table(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> list[dict]:
        """Update KLI data table."""
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = kli_amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
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
    # CSV download callback for KLI
    # ========================================
    @app.callback(
        Output("download-pdb-kli-csv", "data"),
        Input("download-pdb-kli-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_kli_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download KLI data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        if not selected_utilities:
            selected_utilities = []

        filtered_df = kli_amounts_data.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
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

        return {"content": csv_string, "filename": "kli_past_due_balances_export.csv"}
