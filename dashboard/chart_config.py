"""Configuration classes for charts and components.

These data classes help reduce function parameter counts and group related
configuration values together for better maintainability.
"""

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass
class ChartConfig:
    """Configuration for chart display properties.

    Groups common chart configuration parameters to reduce function signatures
    and make chart creation more maintainable.

    Attributes:
        value_column: Name of the column containing values to plot
        y_axis_title: Title text for the y-axis
        is_amount: Whether values represent currency amounts (for formatting)
        height: Chart height in pixels
        add_trendline: Whether to add a trendline to the chart
        trendline_color: Hex color code for the trendline
    """

    value_column: str
    y_axis_title: str
    is_amount: bool = False
    height: int = 550
    add_trendline: bool = False
    trendline_color: str = "#e74c3c"


@dataclass
class ChartCallbackConfig:
    """Configuration for chart callback factories.

    Combines chart configuration with callback-specific IDs to simplify
    callback factory function signatures.

    Attributes:
        chart_id: DOM ID for the chart output element
        subtitle_id: DOM ID for the subtitle/description output element
        config: Chart display configuration
    """

    chart_id: str
    subtitle_id: str
    config: ChartConfig


@dataclass
class KpiCardData:
    """Data for KPI card component display.

    Encapsulates all data needed to render a KPI card, reducing the
    create_kpi_card function from 10 parameters to 1.

    Attributes:
        icon: Emoji or icon character for the card header
        title: Title text for the metric
        value: Main metric value to display (pre-formatted)
        date_range: Date range text (e.g., "Q3 2024 (Jul-Sep)")
        qoq_change: Quarter-over-quarter change text (e.g., "+2,150 (+5.0%)")
        qoq_arrow: Arrow symbol for QoQ trend (↑ or ↓)
        qoq_color: CSS color class or value for QoQ comparison
        yoy_change: Year-over-year change text (e.g., "+3,890 (+9.4%)")
        yoy_arrow: Arrow symbol for YoY trend (↑ or ↓)
        yoy_color: CSS color class or value for YoY comparison
    """

    icon: str
    title: str
    value: str
    date_range: str
    qoq_change: str
    qoq_arrow: str
    qoq_color: str
    yoy_change: str
    yoy_arrow: str
    yoy_color: str


@dataclass
class CallbackInputs:
    """Standard date range and utility filter inputs for callbacks.

    Many callbacks use the same set of inputs (date pickers and utility selection).
    This class provides a consistent structure for these common inputs.

    Attributes:
        start_month: Starting month (1-12)
        start_year: Starting year
        end_month: Ending month (1-12)
        end_year: Ending year
        selected_utilities: List of selected utility names
    """

    start_month: int
    start_year: int
    end_month: int
    end_year: int
    selected_utilities: list[str]

    def to_date_range(self) -> tuple[datetime, datetime]:
        """Convert to start_date and end_date datetime objects.

        Returns:
            Tuple of (start_date, end_date) as datetime objects with UTC timezone
        """
        start_date = datetime(self.start_year, self.start_month, 1, tzinfo=UTC)
        end_date = datetime(self.end_year, self.end_month, 1, tzinfo=UTC)
        return start_date, end_date
