"""Dynamic callback generation for datasets."""

from datetime import UTC, datetime

import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output

from dashboard.chart_builders import create_stacked_line_chart
from dashboard.dashboard_config import DatasetConfig, load_dataset


def create_dataset_callbacks(app: Dash, config: DatasetConfig, all_utilities: list[str]) -> None:
    """Create callbacks for a dataset's chart and table.

    Args:
        app: Dash app instance
        config: Dataset configuration
        all_utilities: List of all utility names
    """
    dataset_id = config.file_name.replace("_", "-")

    # Load the dataset
    dataset = load_dataset(config.file_name)

    # Callback to update chart and table
    @app.callback(
        [
            Output(f"{dataset_id}-chart-subtitle", "children"),
            Output(f"{dataset_id}-chart", "figure"),
            Output(f"{dataset_id}-table", "data"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_dataset_dashboard(
        start_month: int, start_year: int, end_month: int, end_year: int, selected_utilities: list[str]
    ) -> tuple[str, go.Figure, list[dict]]:
        """Update the chart and table based on filter selections."""
        # Convert month/year to datetime objects for display
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        # Ensure we have a list of utilities - if empty, show NO data
        if not selected_utilities:
            selected_utilities = []

        # Add a Date column for easier filtering and charting
        dataset_with_date = dataset.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))

        # Filter by date range first
        filtered_df = dataset_with_date.filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        # Then filter by utilities - if none selected, return empty dataframe
        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            # No utilities selected - return empty dataframe
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Aggregate data for the stacked area chart
        chart_data = filtered_df.group_by(["Date", "Utility"]).agg(pl.col(config.value_column).sum()).sort("Date")

        # Create chart subtitle
        chart_subtitle = f"{start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}"

        # Display-friendly axis title: replace 'Arrearage' with 'Past-Due Balance' for UI
        display_value_label = config.value_column.replace("Arrearage", "Past-Due Balance")
        y_axis_title = f"{display_value_label} ($)" if config.is_amount else display_value_label

        colors = {"PSE": "#156570", "Avista": "#B4CEB3", "PAC": "#9B7EDE", "CNG": "#FE5F55", "NWN": "#5C415D"}

        # Create figure using centralized chart builder
        utilities_to_show = selected_utilities if selected_utilities else all_utilities
        fig = create_stacked_line_chart(
            data=chart_data,
            utilities=utilities_to_show,
            value_column=config.value_column,
            y_axis_title=y_axis_title,
            utility_colors=colors,
            is_amount=config.is_amount,
            height=550,
        )

        # Prepare table data
        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        # Remove helper columns but keep Vintage if it exists
        cols_to_drop = ["Date", "Year"]
        table_df = table_df.drop(columns=[col for col in cols_to_drop if col in table_df.columns])
        table_data = table_df.to_dict("records")

        return chart_subtitle, fig, table_data

    # Callback for CSV download
    @app.callback(
        Output(f"download-{dataset_id}-csv", "data"),
        Input(f"download-{dataset_id}-btn", "n_clicks"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_dataset_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str],
    ) -> dict | None:
        """Download the filtered data as CSV."""
        if n_clicks is None or n_clicks == 0:
            return None

        # Convert month/year to datetime objects
        start_date = datetime(start_year, start_month, 1, tzinfo=UTC)
        end_date = datetime(end_year, end_month, 1, tzinfo=UTC)

        # Ensure we have a list of utilities - if empty, show NO data
        if not selected_utilities:
            selected_utilities = []

        # Add a Date column for easier filtering
        dataset_with_date = dataset.with_columns(pl.date(pl.col("Year"), pl.col("Month"), 1).alias("Date"))

        # Filter by date range first
        filtered_df = dataset_with_date.filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

        # Then filter by utilities - if none selected, return empty dataframe
        if selected_utilities:
            filtered_df = filtered_df.filter(pl.col("Utility").is_in(selected_utilities))
        else:
            # No utilities selected - return empty dataframe
            filtered_df = filtered_df.filter(pl.lit(value=False))

        # Convert to pandas and prepare for download
        table_df = filtered_df.to_pandas()
        table_df["Month"] = table_df["Date"].dt.strftime("%b %Y")
        table_df = table_df.drop(columns=["Date"])  # Remove helper column but keep Year
        csv_string = table_df.to_csv(index=False)

        return {"content": csv_string, "filename": f"{config.file_name}_export.csv"}
