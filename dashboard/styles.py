"""Centralized style definitions for dashboard components."""

# ==========================================
# CARD AND CONTAINER STYLES
# ==========================================

CARD_STYLE = {
    "padding": "25px",
    "backgroundColor": "white",
    "borderRadius": "8px",
    "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
    "border": "1px solid #e1e8ed",
    "marginBottom": "25px",
}

# ==========================================
# TEXT STYLES
# ==========================================

TITLE_STYLE = {
    "marginTop": 0,
    "marginBottom": "5px",
    "color": "#2c3e50",
    "fontSize": "22px",
    "fontWeight": "600",
}

SUBTITLE_STYLE = {
    "color": "#7f8c8d",
    "fontSize": "14px",
    "margin": "0 0 20px 0",
}

SECTION_HEADER_STYLE = {
    "marginTop": "40px",
    "marginBottom": "20px",
    "paddingBottom": "10px",
    "color": "#2c3e50",
    "fontSize": "26px",
    "fontWeight": "700",
    "borderBottom": "3px solid #156570",
}

# ==========================================
# BUTTON STYLES
# ==========================================

DOWNLOAD_BUTTON_STYLE = {
    "float": "right",
    "padding": "8px 16px",
    "backgroundColor": "#156570",
    "color": "white",
    "border": "none",
    "borderRadius": "4px",
    "cursor": "pointer",
    "fontSize": "14px",
    "fontWeight": "500",
}

# ==========================================
# DATA TABLE STYLES
# ==========================================

TABLE_STYLE_CELL = {
    "textAlign": "left",
    "padding": "12px",
    "fontFamily": "Arial, sans-serif",
    "fontSize": "14px",
    "border": "1px solid #e1e8ed",
}

TABLE_STYLE_HEADER = {
    "backgroundColor": "#f8f9fa",
    "fontWeight": "600",
    "textAlign": "left",
    "padding": "12px",
    "border": "1px solid #dee2e6",
    "color": "#2c3e50",
}

TABLE_STYLE_DATA_CONDITIONAL = [
    {
        "if": {"row_index": "odd"},
        "backgroundColor": "#f8f9fa",
    }
]

TABLE_CONTAINER_STYLE = {
    "marginBottom": "20px",
    "overflow": "auto",
}

# ==========================================
# CHART LAYOUT DEFAULTS
# ==========================================

CHART_LAYOUT_DEFAULTS = {
    "plot_bgcolor": "white",
    "paper_bgcolor": "white",
    "font": {"family": "Arial, sans-serif", "size": 12},
    "hovermode": "x unified",
    "legend": {
        "orientation": "h",
        "yanchor": "bottom",
        "y": 1.02,
        "xanchor": "right",
        "x": 1,
    },
    "margin": {"l": 60, "r": 30, "t": 30, "b": 60},
}

CHART_GRID_STYLE = {
    "showgrid": True,
    "gridwidth": 1,
    "gridcolor": "#e1e8ed",
}

# Height configurations for different chart types
CHART_HEIGHTS = {
    "standard": 550,
    "tall": 650,
    "compact": 450,
    "individual_grid": 400,  # Per subplot in grid
}

# ==========================================
# LOADING COMPONENT STYLE
# ==========================================

LOADING_CONFIG = {
    "type": "default",
    "color": "#156570",
}

# ==========================================
# TAB STYLES
# ==========================================

TAB_STYLE = {
    "padding": "12px 30px",
    "fontWeight": "500",
    "fontSize": "16px",
}

TAB_SELECTED_STYLE = {
    "padding": "12px 30px",
    "fontWeight": "700",
    "fontSize": "16px",
    "borderTop": "4px solid #156570",
    "backgroundColor": "white",
}

# ==========================================
# UTILITY-SPECIFIC COLORS
# ==========================================
# Note: These are imported from dashboard_config.py for consistency
# Import as: from dashboard.dashboard_config import UTILITY_COLORS

# ==========================================
# CHART-SPECIFIC CONFIGURATIONS
# ==========================================


def get_line_chart_layout(
    y_axis_title: str,
    height: int = 550,
    is_amount: bool = False,
) -> dict:
    """Get standardized layout configuration for line charts.

    Args:
        y_axis_title: Title for y-axis
        height: Chart height in pixels
        is_amount: Whether y-axis represents currency amounts

    Returns:
        Dictionary of layout parameters
    """
    layout = {
        **CHART_LAYOUT_DEFAULTS,
        "height": height,
        "xaxis_title": "Date",
        "yaxis_title": y_axis_title,
    }

    # Add currency formatting if needed
    if is_amount:
        layout["yaxis"] = {
            "tickformat": "$,.0f",
            **CHART_GRID_STYLE,
        }
        layout["xaxis"] = CHART_GRID_STYLE
    else:
        layout["xaxis"] = CHART_GRID_STYLE
        layout["yaxis"] = {
            "tickformat": ",",
            **CHART_GRID_STYLE,
        }

    return layout


def get_bar_chart_layout(
    y_axis_title: str,
    height: int = 550,
    is_amount: bool = False,
    barmode: str = "stack",
) -> dict:
    """Get standardized layout configuration for bar charts.

    Args:
        y_axis_title: Title for y-axis
        height: Chart height in pixels
        is_amount: Whether y-axis represents currency amounts
        barmode: Bar mode ('stack', 'group', 'overlay')

    Returns:
        Dictionary of layout parameters
    """
    layout = get_line_chart_layout(y_axis_title, height, is_amount)
    layout["barmode"] = barmode

    return layout


def get_subplot_layout(
    n_rows: int,
    height_per_row: int = 400,
) -> dict:
    """Get standardized layout configuration for subplots.

    Args:
        n_rows: Number of subplot rows
        n_cols: Number of subplot columns
        height_per_row: Height per row in pixels
        shared_xaxes: Whether to share x-axes

    Returns:
        Dictionary of layout parameters
    """
    total_height = n_rows * height_per_row

    layout = {
        **CHART_LAYOUT_DEFAULTS,
        "height": total_height,
        "showlegend": True,
    }

    # Remove default margin/legend positioning for subplots
    layout.pop("legend", None)
    layout["margin"] = {"l": 60, "r": 30, "t": 40, "b": 60}

    return layout
