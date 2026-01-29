"""Callbacks for Bill Assistance - Enrollment visualizations."""

from datetime import UTC, datetime

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output, State, dcc
from dash.exceptions import PreventUpdate
from plotly.subplots import make_subplots
from scipy import stats

from dashboard.chart_builders import create_stacked_line_chart


def register_enrollment_callbacks(
    app: Dash,
    df_bill_assist: pl.DataFrame,
    utility_colors: dict[str, str],
) -> None:
    """Register callbacks for bill assistance enrollment visualizations.

    Args:
        app: Dash app instance
        df_bill_assist: Bill assistance dataset
        utility_colors: Utility color mapping
    """

    # Chart 1: Stacked line graph - bill assistance by utility
    @app.callback(
        [
            Output("ba-enrollment-by-utility-stacked-line", "figure"),
            Output("ba-enrollment-by-utility-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_ba_enrollment_stacked_line(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> tuple[go.Figure, str]:
        """Create stacked line graph for bill assistance enrollment by utility."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        # Filter by date range and utilities
        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Aggregate by utility and date
        df_agg = (
            df_filtered.group_by(["Utility", "Date"])
            .agg(pl.col("Bill Assist Customer Count").sum())
            .sort(["Utility", "Date"])
        )

        # Create figure using centralized chart builder
        fig = create_stacked_line_chart(
            data=df_agg,
            utilities=selected_utilities,
            value_column="Bill Assist Customer Count",
            y_axis_title="Number of Customers",
            utility_colors=utility_colors,
            is_amount=False,
            height=500,
        )

        # Create subtitle
        total = df_agg["Bill Assist Customer Count"].sum()
        subtitle = f"Total customers enrolled: {total:,.0f}"

        return fig, subtitle

    # Chart 2: Individual utility trendlines
    @app.callback(
        [
            Output("ba-enrollment-individual-trendlines", "figure"),
            Output("ba-enrollment-individual-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_ba_enrollment_individual_trendlines(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> tuple[go.Figure, str]:
        """Create individual trendlines for each utility."""
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
            return empty_fig, "No utilities selected"

        # Create date range
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        # Filter by date range and utilities
        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Aggregate by utility and date
        df_agg = (
            df_filtered.group_by(["Utility", "Date"])
            .agg(pl.col("Bill Assist Customer Count").sum())
            .sort(["Utility", "Date"])
        )

        # Calculate grid dimensions
        n_utils = len(selected_utilities)
        n_cols = 2 if n_utils > 1 else 1
        n_rows = (n_utils + 1) // 2

        # Create subplots
        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            subplot_titles=selected_utilities,
            vertical_spacing=0.12,
            horizontal_spacing=0.1,
        )

        for idx, utility in enumerate(selected_utilities):
            row = (idx // n_cols) + 1
            col = (idx % n_cols) + 1

            utility_data = df_agg.filter(pl.col("Utility") == utility).sort("Date")

            if len(utility_data) > 1:
                # Convert dates to numeric for trendline
                dates = utility_data["Date"].to_list()
                x_numeric = [(d - dates[0]).days for d in dates]
                y_values = utility_data["Bill Assist Customer Count"].to_list()

                # Calculate trendline
                slope, intercept, _, _, _ = stats.linregress(x_numeric, y_values)
                trendline_y = [slope * x + intercept for x in x_numeric]

                # Add actual data
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=y_values,
                        mode="lines+markers",
                        name=utility,
                        line={"color": utility_colors.get(utility, "#95a5a6"), "width": 2},
                        marker={"size": 6},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )

                # Add trendline
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=trendline_y,
                        mode="lines",
                        name="Trend",
                        line={"color": "rgba(255, 99, 71, 0.5)", "width": 2, "dash": "dash"},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )

        fig.update_layout(
            height=300 * n_rows,
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin={"l": 60, "r": 30, "t": 50, "b": 60},
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

        subtitle = f"Showing trends for {len(selected_utilities)} utilities"

        return fig, subtitle

    # Data table callback
    @app.callback(
        Output("ba-enrollment-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_ba_enrollment_table(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> list[dict]:
        """Update bill assistance enrollment data table."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Format Month column as "Jan 2025" and convert Date to ISO for sorting
        df_formatted = df_filtered.with_columns(
            [
                pl.col("Date").dt.strftime("%Y-%m-%d").alias("Date"),
                pl.col("Date").dt.strftime("%b %Y").alias("Month"),
            ]
        ).drop("Year")
        df_sorted = df_formatted.sort(["Utility", "Date", "Zip Code"])

        return df_sorted.to_dicts()

    # CSV download callback
    @app.callback(
        Output("download-ba-enrollment-csv", "data"),
        Input("download-ba-enrollment-btn", "n_clicks"),
        [
            State("start-month-picker", "value"),
            State("start-year-picker", "value"),
            State("end-month-picker", "value"),
            State("end-year-picker", "value"),
            State("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_ba_enrollment_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> dict:
        """Download bill assistance enrollment data as CSV."""
        if not n_clicks or not selected_utilities:
            raise PreventUpdate

        # Create date range
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))

        # Format Month column as "Jan 2025" for CSV
        df_formatted = df_filtered.with_columns(pl.col("Date").dt.strftime("%b %Y").alias("Month")).drop("Year")
        df_sorted = df_formatted.sort(["Utility", "Date", "Zip Code"])

        return dcc.send_data_frame(
            df_sorted.to_pandas().to_csv,
            "bill_assistance_enrollment.csv",
            index=False,
        )
