"""Chart builder functions for creating standardized Plotly visualizations."""

import polars as pl
from plotly import graph_objects as go
from plotly.subplots import make_subplots

from dashboard.styles import (
    CHART_GRID_STYLE,
    get_bar_chart_layout,
    get_line_chart_layout,
    get_subplot_layout,
)
from dashboard.utils import calculate_trendline


def create_stacked_line_chart(
    data: pl.DataFrame,
    utilities: list[str],
    value_column: str,
    y_axis_title: str,
    utility_colors: dict[str, str],
    is_amount: bool = False,
    height: int = 550,
) -> go.Figure:
    """Create standardized stacked line chart.

    Args:
        data: Dataframe with columns [Utility, Date, value_column]
        utilities: List of utilities to include (in order)
        value_column: Name of the value column to plot
        y_axis_title: Title for y-axis
        utility_colors: Dictionary mapping utility names to colors
        is_amount: Whether values are currency amounts
        height: Chart height in pixels

    Returns:
        Plotly Figure object
    """
    fig = go.Figure()

    # Add traces for each utility
    for utility in utilities:
        utility_data = data.filter(pl.col("Utility") == utility)

        if len(utility_data) > 0:
            fig.add_trace(
                go.Scatter(
                    x=utility_data["Date"].to_list(),
                    y=utility_data[value_column].to_list(),
                    mode="lines",
                    name=utility,
                    line={
                        "width": 3,
                        "color": utility_colors.get(utility, "#95a5a6"),
                    },
                    stackgroup="one",
                )
            )

    # Apply layout
    layout = get_line_chart_layout(y_axis_title, height, is_amount)
    fig.update_layout(layout)

    return fig


def create_stacked_area_chart(
    data: pl.DataFrame,
    utilities: list[str],
    value_column: str,
    y_axis_title: str,
    utility_colors: dict[str, str],
    is_amount: bool = False,
    height: int = 550,
) -> go.Figure:
    """Create stacked area chart (alias for stacked line chart).

    This is identical to stacked line chart but provided for semantic clarity.
    """
    return create_stacked_line_chart(data, utilities, value_column, y_axis_title, utility_colors, is_amount, height)


def create_individual_trendlines(
    data: pl.DataFrame,
    utilities: list[str],
    value_column: str,
    utility_colors: dict[str, str],
    utility_display_names: dict[str, str],
    is_amount: bool = False,
    n_cols: int = 2,
    height_per_row: int = 400,
) -> go.Figure:
    """Create individual trendline charts in a grid layout.

    Args:
        data: Dataframe with columns [Utility, Date, value_column]
        utilities: List of utilities to include
        value_column: Name of the value column to plot
        utility_colors: Dictionary mapping utility names to colors
        utility_display_names: Dictionary mapping utility codes to full names
        is_amount: Whether values are currency amounts
        n_cols: Number of columns in grid
        height_per_row: Height per row in pixels

    Returns:
        Plotly Figure object with subplots
    """
    n_utilities = len(utilities)
    n_rows = (n_utilities + n_cols - 1) // n_cols  # Ceiling division

    # Create subplots
    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=[utility_display_names.get(u, u) for u in utilities],
        vertical_spacing=0.12,
        horizontal_spacing=0.10,
    )

    # Add traces for each utility
    for idx, utility in enumerate(utilities):
        row = idx // n_cols + 1
        col = idx % n_cols + 1

        utility_data = data.filter(pl.col("Utility") == utility).sort("Date")

        if len(utility_data) > 0:
            dates = utility_data["Date"].to_list()
            y_values = utility_data[value_column].to_list()
            color = utility_colors.get(utility, "#95a5a6")

            # Add main line
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=y_values,
                    mode="lines",
                    name=utility,
                    line={"width": 3, "color": color},
                    showlegend=False,
                ),
                row=row,
                col=col,
            )

            # Calculate and add trendline
            if len(dates) >= 2:
                trendline_y, _, _ = calculate_trendline(dates, y_values)

                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=trendline_y,
                        mode="lines",
                        name="Trendline",
                        line={"width": 2, "color": "#e74c3c", "dash": "dash"},
                        showlegend=False,
                    ),
                    row=row,
                    col=col,
                )

    # Apply layout
    layout = get_subplot_layout(n_rows, height_per_row)
    fig.update_layout(layout)

    # Update axes
    for i in range(1, n_utilities + 1):
        fig.update_xaxes(
            showgrid=True, gridwidth=1, gridcolor="#e1e8ed", row=(i - 1) // n_cols + 1, col=(i - 1) % n_cols + 1
        )
        y_axis_config = {"showgrid": True, "gridwidth": 1, "gridcolor": "#e1e8ed"}
        if is_amount:
            y_axis_config["tickformat"] = "$,.0f"
        else:
            y_axis_config["tickformat"] = ","
        fig.update_yaxes(**y_axis_config, row=(i - 1) // n_cols + 1, col=(i - 1) % n_cols + 1)

    return fig


def create_single_trendline_chart(
    data: pl.DataFrame,
    value_column: str,
    y_axis_title: str,
    line_color: str = "#2596be",
    trendline_color: str = "#e74c3c",
    is_amount: bool = False,
    height: int = 550,
) -> go.Figure:
    """Create single line chart with trendline (for aggregated data).

    Args:
        data: Dataframe with columns [Date, value_column]
        value_column: Name of the value column to plot
        y_axis_title: Title for y-axis
        line_color: Color for main line
        trendline_color: Color for trendline
        is_amount: Whether values are currency amounts
        height: Chart height in pixels

    Returns:
        Plotly Figure object
    """
    fig = go.Figure()

    dates = data["Date"].to_list()
    y_values = data[value_column].to_list()

    # Add main line
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=y_values,
            mode="lines",
            name="Actual",
            line={"width": 3, "color": line_color},
        )
    )

    # Calculate and add trendline
    if len(dates) >= 2:
        trendline_y, _, _ = calculate_trendline(dates, y_values)

        fig.add_trace(
            go.Scatter(
                x=dates,
                y=trendline_y,
                mode="lines",
                name="Trendline",
                line={"width": 2, "color": trendline_color, "dash": "dash"},
            )
        )

    # Apply layout
    layout = get_line_chart_layout(y_axis_title, height, is_amount)
    fig.update_layout(layout)

    return fig


def create_stacked_bar_chart(
    data: pl.DataFrame,
    utilities: list[str],
    value_column: str,
    y_axis_title: str,
    utility_colors: dict[str, str],
    is_amount: bool = False,
    height: int = 550,
    add_trendline: bool = False,
    trendline_color: str = "#e74c3c",
) -> go.Figure:
    """Create stacked bar chart.

    Args:
        data: Dataframe with columns [Utility, Date, value_column]
        utilities: List of utilities to include (in order)
        value_column: Name of the value column to plot
        y_axis_title: Title for y-axis
        utility_colors: Dictionary mapping utility names to colors
        is_amount: Whether values are currency amounts
        height: Chart height in pixels
        add_trendline: Whether to add a trendline to the total
        trendline_color: Color for trendline

    Returns:
        Plotly Figure object
    """
    fig = go.Figure()

    # Add bar traces for each utility
    for utility in utilities:
        utility_data = data.filter(pl.col("Utility") == utility)

        if len(utility_data) > 0:
            fig.add_trace(
                go.Bar(
                    x=utility_data["Date"].to_list(),
                    y=utility_data[value_column].to_list(),
                    name=utility,
                    marker_color=utility_colors.get(utility, "#95a5a6"),
                )
            )

    # Add trendline if requested
    if add_trendline:
        # Calculate total across utilities
        total_data = data.group_by("Date").agg(pl.col(value_column).sum()).sort("Date")

        dates = total_data["Date"].to_list()
        y_values = total_data[value_column].to_list()

        if len(dates) >= 2:
            trendline_y, _, _ = calculate_trendline(dates, y_values)

            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=trendline_y,
                    mode="lines",
                    name="Trendline",
                    line={"width": 3, "color": trendline_color, "dash": "dash"},
                    yaxis="y",
                )
            )

    # Apply layout
    layout = get_bar_chart_layout(y_axis_title, height, is_amount, barmode="stack")
    fig.update_layout(layout)

    return fig


def create_grouped_bar_chart(
    data: pl.DataFrame,
    utilities: list[str],
    value_column: str,
    y_axis_title: str,
    utility_colors: dict[str, str],
    is_amount: bool = False,
    height: int = 550,
) -> go.Figure:
    """Create grouped (clustered) bar chart.

    Args:
        data: Dataframe with columns [Utility, Date, value_column]
        utilities: List of utilities to include (in order)
        value_column: Name of the value column to plot
        y_axis_title: Title for y-axis
        utility_colors: Dictionary mapping utility names to colors
        is_amount: Whether values are currency amounts
        height: Chart height in pixels

    Returns:
        Plotly Figure object
    """
    fig = go.Figure()

    # Add bar traces for each utility
    for utility in utilities:
        utility_data = data.filter(pl.col("Utility") == utility)

        if len(utility_data) > 0:
            fig.add_trace(
                go.Bar(
                    x=utility_data["Date"].to_list(),
                    y=utility_data[value_column].to_list(),
                    name=utility,
                    marker_color=utility_colors.get(utility, "#95a5a6"),
                )
            )

    # Apply layout
    layout = get_bar_chart_layout(y_axis_title, height, is_amount, barmode="group")
    fig.update_layout(layout)

    return fig


def create_pie_chart(
    data: pl.DataFrame,
    names_column: str,
    values_column: str,
    colors: list[str] | None = None,
    height: int = 550,
) -> go.Figure:
    """Create pie chart.

    Args:
        data: Dataframe with names and values columns
        names_column: Name of the column containing category names
        values_column: Name of the column containing values
        colors: List of colors for slices (optional)
        height: Chart height in pixels

    Returns:
        Plotly Figure object
    """
    fig = go.Figure()

    fig.add_trace(
        go.Pie(
            labels=data[names_column].to_list(),
            values=data[values_column].to_list(),
            marker={"colors": colors} if colors else {},
            textposition="inside",
            textinfo="label+percent+value",
        )
    )

    fig.update_layout(
        height=height,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"family": "Arial, sans-serif", "size": 12},
        margin={"l": 30, "r": 30, "t": 30, "b": 30},
    )

    return fig
