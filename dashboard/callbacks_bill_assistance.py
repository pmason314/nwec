"""Callbacks for Bill Assistance tab."""

from datetime import datetime
from pathlib import Path

import polars as pl
from dash import Input, Output, State, dcc
from dash.exceptions import PreventUpdate
from plotly import graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats

from dashboard.dashboard_config import load_dataset


def create_bill_assistance_callbacks(app, all_utilities: list):
    """Register all Bill Assistance callbacks."""
    # Load data
    df_bill_assist = load_dataset("bill_assist")
    df_payment_arr = load_dataset("payment_agreements")

    # Utility colors
    utility_colors = {
        "Avista": "#003768",
        "PSE": "#2596be",
        "PAC": "#e90028",
        "CNG": "#6c757d",
        "NWN": "#54b266",
    }

    # ====================
    # BILL ASSISTANCE ENROLLMENT
    # ====================

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
    def update_ba_enrollment_stacked_line(start_month, start_year, end_month, end_year, selected_utilities):
        """Create stacked line graph for bill assistance enrollment by utility."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

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

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities:
            utility_data = df_agg.filter(pl.col("Utility") == utility)

            fig.add_trace(
                go.Scatter(
                    x=utility_data["Date"].to_list(),
                    y=utility_data["Bill Assist Customer Count"].to_list(),
                    mode="lines",
                    name=utility,
                    line=dict(width=3, color=utility_colors.get(utility, "#95a5a6")),
                    stackgroup="one",
                )
            )

        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Number of Customers",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=60, r=30, t=30, b=60),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=12),
            height=500,
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

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
    def update_ba_enrollment_individual_trendlines(start_month, start_year, end_month, end_year, selected_utilities):
        """Create individual trendlines for each utility."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

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
                slope, intercept, r_value, _, _ = stats.linregress(x_numeric, y_values)
                trendline_y = [slope * x + intercept for x in x_numeric]

                # Add actual data
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=y_values,
                        mode="lines+markers",
                        name=utility,
                        line=dict(color=utility_colors.get(utility, "#95a5a6"), width=2),
                        marker=dict(size=6),
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
                        line=dict(color="rgba(255, 99, 71, 0.5)", width=2, dash="dash"),
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
            margin=dict(l=60, r=30, t=50, b=60),
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

        subtitle = f"Showing trends for {len(selected_utilities)} utilities"

        return fig, subtitle

    # ====================
    # PAYMENT ARRANGEMENTS
    # ====================

    # Chart 3: Stacked line graph - payment arrangements by utility
    @app.callback(
        [
            Output("ba-payment-arr-by-utility-stacked-line", "figure"),
            Output("ba-payment-arr-by-utility-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_payment_arr_stacked_line(start_month, start_year, end_month, end_year, selected_utilities):
        """Create stacked line graph for payment arrangements by utility."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        # Filter by date range and utilities
        df_filtered = df_payment_arr.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Aggregate by utility and date
        df_agg = (
            df_filtered.group_by(["Utility", "Date"])
            .agg(pl.col("Payment Agreement Customer Count").sum())
            .sort(["Utility", "Date"])
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities:
            utility_data = df_agg.filter(pl.col("Utility") == utility)

            fig.add_trace(
                go.Scatter(
                    x=utility_data["Date"].to_list(),
                    y=utility_data["Payment Agreement Customer Count"].to_list(),
                    mode="lines",
                    name=utility,
                    line=dict(width=3, color=utility_colors.get(utility, "#95a5a6")),
                    stackgroup="one",
                )
            )

        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Number of Customers",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=60, r=30, t=30, b=60),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=12),
            height=500,
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

        # Create subtitle
        total = df_agg["Payment Agreement Customer Count"].sum()
        subtitle = f"Total customers with payment arrangements: {total:,.0f}"

        return fig, subtitle

    # Chart 4: Individual utility trendlines for payment arrangements
    @app.callback(
        [
            Output("ba-payment-arr-individual-trendlines", "figure"),
            Output("ba-payment-arr-individual-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_payment_arr_individual_trendlines(start_month, start_year, end_month, end_year, selected_utilities):
        """Create individual trendlines for payment arrangements."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        # Filter by date range and utilities
        df_filtered = df_payment_arr.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Aggregate by utility and date
        df_agg = (
            df_filtered.group_by(["Utility", "Date"])
            .agg(pl.col("Payment Agreement Customer Count").sum())
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
                y_values = utility_data["Payment Agreement Customer Count"].to_list()

                # Calculate trendline
                slope, intercept, r_value, _, _ = stats.linregress(x_numeric, y_values)
                trendline_y = [slope * x + intercept for x in x_numeric]

                # Add actual data
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=y_values,
                        mode="lines+markers",
                        name=utility,
                        line=dict(color=utility_colors.get(utility, "#95a5a6"), width=2),
                        marker=dict(size=6),
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
                        line=dict(color="rgba(255, 99, 71, 0.5)", width=2, dash="dash"),
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
            margin=dict(l=60, r=30, t=50, b=60),
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

        subtitle = f"Showing trends for {len(selected_utilities)} utilities"

        return fig, subtitle

    # ====================
    # DATA TABLES
    # ====================

    # Bill Assistance Enrollment Table
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
    def update_ba_enrollment_table(start_month, start_year, end_month, end_year, selected_utilities):
        """Update bill assistance enrollment data table."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Sort for better readability
        df_sorted = df_filtered.sort(["Utility", "Month", "Zip Code"])

        return df_sorted.to_dicts()

    # Payment Arrangements Table
    @app.callback(
        Output("ba-payment-arr-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_payment_arr_table(start_month, start_year, end_month, end_year, selected_utilities):
        """Update payment arrangements data table."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_payment_arr.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Sort for better readability
        df_sorted = df_filtered.sort(["Utility", "Month", "Zip Code"])

        return df_sorted.to_dicts()

    # ====================
    # CSV DOWNLOADS
    # ====================

    # Bill Assistance Enrollment CSV Download
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
    def download_ba_enrollment_csv(n_clicks, start_month, start_year, end_month, end_year, selected_utilities):
        """Download bill assistance enrollment data as CSV."""
        if not n_clicks or not selected_utilities:
            raise PreventUpdate

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_bill_assist.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))

        df_sorted = df_filtered.sort(["Utility", "Month", "Zip Code"])

        return dcc.send_data_frame(
            df_sorted.to_pandas().to_csv,
            "bill_assistance_enrollment.csv",
            index=False,
        )

    # Payment Arrangements CSV Download
    @app.callback(
        Output("download-ba-payment-arr-csv", "data"),
        Input("download-ba-payment-arr-btn", "n_clicks"),
        [
            State("start-month-picker", "value"),
            State("start-year-picker", "value"),
            State("end-month-picker", "value"),
            State("end-year-picker", "value"),
            State("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_payment_arr_csv(n_clicks, start_month, start_year, end_month, end_year, selected_utilities):
        """Download payment arrangements data as CSV."""
        if not n_clicks or not selected_utilities:
            raise PreventUpdate

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_payment_arr.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))

        df_sorted = df_filtered.sort(["Utility", "Month", "Zip Code"])

        return dcc.send_data_frame(
            df_sorted.to_pandas().to_csv,
            "payment_arrangements.csv",
            index=False,
        )
