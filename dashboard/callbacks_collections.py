"""Callbacks for Collections tab."""

from datetime import datetime

import polars as pl
from dash import Input, Output, State, dcc
from dash.exceptions import PreventUpdate
from plotly import graph_objects as go

from dashboard.dashboard_config import load_dataset


def create_collections_callbacks(app, all_utilities: list):
    """Register all Collections callbacks."""
    # Load data
    df_collections = load_dataset("collection_agency_referrals")

    # Utility colors
    utility_colors = {
        "Avista": "#003768",
        "PSE": "#2596be",
        "PAC": "#e90028",
        "CNG": "#6c757d",
        "NWN": "#54b266",
    }

    # ====================
    # COLLECTION AGENCY REFERRALS
    # ====================

    # Chart: Stacked line graph - collection agency referrals by utility
    @app.callback(
        [
            Output("collections-referrals-by-utility-stacked-line", "figure"),
            Output("collections-referrals-by-utility-subtitle", "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_collections_referrals_stacked_line(start_month, start_year, end_month, end_year, selected_utilities):
        """Create stacked line graph for collection agency referrals by utility."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        # Filter by date range and utilities
        df_filtered = df_collections.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Aggregate by utility and date
        df_agg = (
            df_filtered.group_by(["Utility", "Date"])
            .agg(pl.col("Collections Accounts Customer Count").sum())
            .sort(["Utility", "Date"])
        )

        # Create figure
        fig = go.Figure()

        for utility in selected_utilities:
            utility_data = df_agg.filter(pl.col("Utility") == utility)

            fig.add_trace(
                go.Scatter(
                    x=utility_data["Date"].to_list(),
                    y=utility_data["Collections Accounts Customer Count"].to_list(),
                    mode="lines",
                    name=utility,
                    line={"width": 3, "color": utility_colors.get(utility, "#95a5a6")},
                    stackgroup="one",
                )
            )

        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Number of Customers",
            hovermode="x unified",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            margin={"l": 60, "r": 30, "t": 30, "b": 60},
            plot_bgcolor="white",
            paper_bgcolor="white",
            font={"family": "Arial, sans-serif", "size": 12},
            height=500,
        )

        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#e1e8ed")

        # Create subtitle
        total = df_agg["Collections Accounts Customer Count"].sum()
        subtitle = f"Total customers referred to collection agencies: {total:,.0f}"

        return fig, subtitle

    # ====================
    # DATA TABLE
    # ====================

    # Collection Agency Referrals Table
    @app.callback(
        Output("collections-referrals-data-table", "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_collections_referrals_table(start_month, start_year, end_month, end_year, selected_utilities):
        """Update collection agency referrals data table."""
        if not selected_utilities:
            selected_utilities = []

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_collections.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            df_filtered = df_filtered.filter(pl.lit(value=False))

        # Sort for better readability
        df_sorted = df_filtered.sort(["Utility", "Date", "Zip Code"])

        return df_sorted.to_dicts()

    # ====================
    # CSV DOWNLOAD
    # ====================

    # Collection Agency Referrals CSV Download
    @app.callback(
        Output("download-collections-referrals-csv", "data"),
        Input("download-collections-referrals-btn", "n_clicks"),
        [
            State("start-month-picker", "value"),
            State("start-year-picker", "value"),
            State("end-month-picker", "value"),
            State("end-year-picker", "value"),
            State("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_collections_referrals_csv(n_clicks, start_month, start_year, end_month, end_year, selected_utilities):
        """Download collection agency referrals data as CSV."""
        if not n_clicks or not selected_utilities:
            raise PreventUpdate

        # Create date range
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

        df_filtered = df_collections.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date")).filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if selected_utilities:
            df_filtered = df_filtered.filter(pl.col("Utility").is_in(selected_utilities))

        df_sorted = df_filtered.sort(["Utility", "Date", "Zip Code"])

        return dcc.send_data_frame(
            df_sorted.to_pandas().to_csv,
            "collection_agency_referrals.csv",
            index=False,
        )
