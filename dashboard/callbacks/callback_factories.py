"""Callback factory functions for creating standardized dashboard callbacks."""

from collections.abc import Callable

import polars as pl
from dash import Dash, Input, Output, State, dcc
from dash.exceptions import PreventUpdate
from plotly import graph_objects as go

from dashboard.chart_builders import (
    create_grouped_bar_chart,
    create_individual_trendlines,
    create_single_trendline_chart,
    create_stacked_area_chart,
    create_stacked_bar_chart,
    create_stacked_line_chart,
)
from dashboard.chart_config import ChartCallbackConfig
from dashboard.dashboard_config import UTILITY_DISPLAY_NAMES
from dashboard.utils import (
    UTILITY_COLORS,
    aggregate_by_utility_date,
    filter_by_date_and_utilities,
    get_subtitle_text,
    prepare_table_data,
)


def create_stacked_line_callback(
    app: Dash,
    dataset: pl.DataFrame,
    chart_id: str,
    subtitle_id: str,
    value_column: str,
    y_axis_title: str,
    is_amount: bool = False,
    height: int = 550,
) -> None:
    """Factory to create stacked line chart callback.

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        chart_id: ID for the chart output
        subtitle_id: ID for the subtitle output
        value_column: Name of the column to plot
        y_axis_title: Title for y-axis
        is_amount: Whether values are currency amounts
        height: Chart height in pixels
    """

    @app.callback(
        [
            Output(chart_id, "figure"),
            Output(subtitle_id, "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_chart(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> tuple[go.Figure, str]:
        # Filter data
        df_filtered = filter_by_date_and_utilities(
            dataset, start_month, start_year, end_month, end_year, selected_utilities
        )

        # Aggregate by utility and date
        df_agg = aggregate_by_utility_date(df_filtered, value_column)

        # Create chart
        fig = create_stacked_line_chart(
            data=df_agg,
            utilities=selected_utilities or [],
            value_column=value_column,
            y_axis_title=y_axis_title,
            utility_colors=UTILITY_COLORS,
            is_amount=is_amount,
            height=height,
        )

        # Generate subtitle
        subtitle = get_subtitle_text(
            start_month,
            start_year,
            end_month,
            end_year,
            selected_utilities or [],
            UTILITY_DISPLAY_NAMES,
        )

        return fig, subtitle


def create_individual_trendlines_callback(
    app: Dash,
    dataset: pl.DataFrame,
    chart_id: str,
    subtitle_id: str,
    value_column: str,
    is_amount: bool = False,
    n_cols: int = 2,
    height_per_row: int = 400,
) -> None:
    """Factory to create individual trendlines chart callback.

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        chart_id: ID for the chart output
        subtitle_id: ID for the subtitle output
        value_column: Name of the column to plot
        is_amount: Whether values are currency amounts
        n_cols: Number of columns in grid
        height_per_row: Height per row in pixels
    """

    @app.callback(
        [
            Output(chart_id, "figure"),
            Output(subtitle_id, "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_chart(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> tuple[go.Figure, str]:
        # Filter data
        df_filtered = filter_by_date_and_utilities(
            dataset, start_month, start_year, end_month, end_year, selected_utilities
        )

        # Aggregate by utility and date
        df_agg = aggregate_by_utility_date(df_filtered, value_column)

        # Create chart
        fig = create_individual_trendlines(
            data=df_agg,
            utilities=selected_utilities or [],
            value_column=value_column,
            utility_colors=UTILITY_COLORS,
            utility_display_names=UTILITY_DISPLAY_NAMES,
            is_amount=is_amount,
            n_cols=n_cols,
            height_per_row=height_per_row,
        )

        # Generate subtitle
        subtitle = get_subtitle_text(
            start_month,
            start_year,
            end_month,
            end_year,
            selected_utilities or [],
            UTILITY_DISPLAY_NAMES,
        )

        return fig, subtitle


def create_stacked_bar_callback(
    app: Dash,
    dataset: pl.DataFrame,
    callback_config: ChartCallbackConfig,
) -> None:
    """Factory to create stacked bar chart callback.

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        callback_config: Configuration for chart and callback (chart_id, subtitle_id, and ChartConfig)
    """

    @app.callback(
        [
            Output(callback_config.chart_id, "figure"),
            Output(callback_config.subtitle_id, "children"),
        ],
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_chart(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> tuple[go.Figure, str]:
        # Filter data
        df_filtered = filter_by_date_and_utilities(
            dataset, start_month, start_year, end_month, end_year, selected_utilities
        )

        # Aggregate by utility and date
        df_agg = aggregate_by_utility_date(df_filtered, callback_config.config.value_column)

        # Create chart
        fig = create_stacked_bar_chart(
            data=df_agg,
            utilities=selected_utilities or [],
            utility_colors=UTILITY_COLORS,
            config=callback_config.config,
        )

        # Generate subtitle
        subtitle = get_subtitle_text(
            start_month,
            start_year,
            end_month,
            end_year,
            selected_utilities or [],
            UTILITY_DISPLAY_NAMES,
        )

        return fig, subtitle


def create_data_table_callback(
    app: Dash,
    dataset: pl.DataFrame,
    table_id: str,
    columns_to_keep: list[str] | None = None,
    sort_columns: list[str] | None = None,
) -> None:
    """Factory to create data table callback.

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        table_id: ID for the table output
        columns_to_keep: List of columns to include (None = all)
        sort_columns: List of columns to sort by
    """

    @app.callback(
        Output(table_id, "data"),
        [
            Input("start-month-picker", "value"),
            Input("start-year-picker", "value"),
            Input("end-month-picker", "value"),
            Input("end-year-picker", "value"),
            Input("selected-utilities-store", "data"),
        ],
    )
    def update_table(
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> list[dict]:
        # Filter data
        df_filtered = filter_by_date_and_utilities(
            dataset, start_month, start_year, end_month, end_year, selected_utilities
        )

        # Select columns if specified
        if columns_to_keep:
            existing_cols = [col for col in columns_to_keep if col in df_filtered.columns]
            if existing_cols:
                df_filtered = df_filtered.select(existing_cols)

        # Prepare table data
        return prepare_table_data(df_filtered, sort_columns=sort_columns)


def create_csv_download_callback(
    app: Dash,
    dataset: pl.DataFrame,
    download_id: str,
    download_btn_id: str,
    filename: str,
    columns_to_keep: list[str] | None = None,
    sort_columns: list[str] | None = None,
) -> None:
    """Factory to create CSV download callback.

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        download_id: ID for the download component
        download_btn_id: ID for the download button
        filename: Name for downloaded file
        columns_to_keep: List of columns to include (None = all)
        sort_columns: List of columns to sort by
    """

    @app.callback(
        Output(download_id, "data"),
        Input(download_btn_id, "n_clicks"),
        [
            State("start-month-picker", "value"),
            State("start-year-picker", "value"),
            State("end-month-picker", "value"),
            State("end-year-picker", "value"),
            State("selected-utilities-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def download_csv(
        n_clicks: int | None,
        start_month: int,
        start_year: int,
        end_month: int,
        end_year: int,
        selected_utilities: list[str] | None,
    ) -> dict:
        if not n_clicks or not selected_utilities:
            raise PreventUpdate

        # Filter data
        df_filtered = filter_by_date_and_utilities(
            dataset, start_month, start_year, end_month, end_year, selected_utilities
        )

        # Select columns if specified
        if columns_to_keep:
            existing_cols = [col for col in columns_to_keep if col in df_filtered.columns]
            if existing_cols:
                df_filtered = df_filtered.select(existing_cols)

        # Format and sort
        df_formatted = df_filtered.with_columns(pl.col("Date").dt.strftime("%b %Y").alias("Month"))

        if sort_columns:
            existing_sort_cols = [col for col in sort_columns if col in df_formatted.columns]
            if existing_sort_cols:
                df_formatted = df_formatted.sort(existing_sort_cols)

        # Return CSV
        return dcc.send_data_frame(
            df_formatted.to_pandas().to_csv,
            filename,
            index=False,
        )


def create_simple_callbacks_for_dataset(
    app: Dash,
    dataset: pl.DataFrame,
    prefix: str,
    value_column: str,
    y_axis_title: str,
    table_columns: list[str],
    csv_filename: str,
    is_amount: bool = False,
) -> None:
    """Create a standard set of callbacks for a simple dataset.

    This creates:
    - Stacked line chart callback
    - Individual trendlines callback
    - Data table callback
    - CSV download callback

    Args:
        app: Dash app instance
        dataset: Polars dataframe with data
        prefix: Prefix for component IDs (e.g., "ba-enrollment")
        value_column: Name of the column to plot
        y_axis_title: Title for y-axis
        table_columns: List of columns to include in table
        csv_filename: Name for downloaded CSV file
        is_amount: Whether values are currency amounts
    """
    # Stacked line chart
    create_stacked_line_callback(
        app=app,
        dataset=dataset,
        chart_id=f"{prefix}-stacked-line",
        subtitle_id=f"{prefix}-stacked-subtitle",
        value_column=value_column,
        y_axis_title=y_axis_title,
        is_amount=is_amount,
    )

    # Individual trendlines
    create_individual_trendlines_callback(
        app=app,
        dataset=dataset,
        chart_id=f"{prefix}-individual-trendlines",
        subtitle_id=f"{prefix}-individual-subtitle",
        value_column=value_column,
        is_amount=is_amount,
    )

    # Data table
    create_data_table_callback(
        app=app,
        dataset=dataset,
        table_id=f"{prefix}-data-table",
        columns_to_keep=table_columns,
    )

    # CSV download
    create_csv_download_callback(
        app=app,
        dataset=dataset,
        download_id=f"download-{prefix}-csv",
        download_btn_id=f"download-{prefix}-btn",
        filename=csv_filename,
        columns_to_keep=table_columns,
    )
