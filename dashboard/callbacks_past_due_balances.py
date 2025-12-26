"""Callbacks for Past-Due Balances tab visualizations."""

from datetime import UTC, datetime

import numpy as np
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output
from plotly.subplots import make_subplots
from scipy import stats

from dashboard.dashboard_config import UTILITY_COLORS, load_dataset


def create_past_due_balances_callbacks(app: Dash, all_utilities: list[str]) -> None:
    """Create all callbacks for Past-Due Balances tab.

    Args:
        app: Dash app instance
        all_utilities: List of all utility names
    """
    # Load datasets
    counts_data = load_dataset("arrearage_counts")
    amounts_data = load_dataset("arrearage_amounts")
    kli_amounts_data = load_dataset("kli_arrearage_amounts")

    # Colors for utilities
    colors = UTILITY_COLORS

    # Vintage colors (lighter to darker blue for older debts)
    vintage_colors = {
        "30 Days": "#a8dadc",  # Light blue
        "60 Days": "#457b9d",  # Medium blue
        "90 Days +": "#1d3557",  # Dark blue
        "Total Arrearages": "#2c3e50",  # Very dark (for pie charts)
    }

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
                y_values = utility_data["Arrearage Customer Count"].values
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
        chart_data = (
            filtered_df.group_by(["Date", "Utility"]).agg(pl.col("Arrearage_Amount").sum()).sort("Date").to_pandas()
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities if selected_utilities else all_utilities:
            utility_data = chart_data[chart_data["Utility"] == utility].sort_values("Date")
            if not utility_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=utility_data["Date"],
                        y=utility_data["Arrearage_Amount"],
                        name=utility,
                        mode="lines",
                        stackgroup="one",
                        fillcolor=colors.get(utility, "#cccccc"),
                        line={"width": 0.5, "color": colors.get(utility, "#cccccc")},
                        hovertemplate=f"<b>{utility}</b><br>${{y:,.2f}}<extra></extra>",
                    )
                )

        fig.update_layout(
            xaxis_title="",
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
            y_values = total_by_date["Arrearage_Amount"].values
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
                    marker=dict(colors=[vintage_colors.get(v, "#cccccc") for v in chart_data["Vintage"]]),
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

    # Import pandas for categorical data
    import pandas as pd

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
            y_values = total_by_date["Arrearage_Amount"].values
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
                    marker=dict(colors=[vintage_colors.get(v, "#cccccc") for v in chart_data["Vintage"]]),
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
    # Data table callbacks
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

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Arrearage Customer Count"]]
        return table_df.to_dict("records")

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

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Vintage", "Arrearage_Amount"]]
        return table_df.to_dict("records")

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

        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df[["Utility", "Zip Code", "Month", "Vintage", "Arrearage_Amount"]]
        return table_df.to_dict("records")

    # ========================================
    # CSV download callbacks
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
