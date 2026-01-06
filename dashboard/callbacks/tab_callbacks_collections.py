"""Callbacks for Collections tab (Refactored)."""

from typing import TYPE_CHECKING

from dashboard.callbacks.callback_factories import (
    create_csv_download_callback,
    create_data_table_callback,
    create_stacked_line_callback,
)
from dashboard.dashboard_config import load_dataset

if TYPE_CHECKING:
    from dash import Dash


def create_collections_callbacks(app: "Dash") -> None:
    """Register all Collections callbacks using factory functions."""
    # Load data
    df_collections = load_dataset("collection_agency_referrals")

    # Define configuration
    value_column = "Collections Accounts Customer Count"
    y_axis_title = "Number of Customers"
    table_columns = ["Utility", "Date", "Zip Code", value_column]
    sort_columns = ["Utility", "Date", "Zip Code"]  # Sort by Date for chronological order

    # ====================
    # COLLECTION AGENCY REFERRALS
    # ====================

    # Stacked line chart
    create_stacked_line_callback(
        app=app,
        dataset=df_collections,
        chart_id="collections-referrals-by-utility-stacked-line",
        subtitle_id="collections-referrals-by-utility-subtitle",
        value_column=value_column,
        y_axis_title=y_axis_title,
        is_amount=False,
        height=500,
    )

    # Data table
    create_data_table_callback(
        app=app,
        dataset=df_collections,
        table_id="collections-referrals-data-table",
        columns_to_keep=table_columns,
        sort_columns=sort_columns,
    )

    # CSV download
    create_csv_download_callback(
        app=app,
        dataset=df_collections,
        download_id="download-collections-referrals-csv",
        download_btn_id="download-collections-referrals-btn",
        filename="collection_agency_referrals.csv",
        columns_to_keep=table_columns,
        sort_columns=sort_columns,
    )
