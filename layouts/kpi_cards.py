"""KPI cards section with card builder components."""

from dash import html


def create_kpi_cards_section() -> html.Div:
    """Create the KPI cards section container (populated by callback)."""
    return html.Div(
        id="kpi-cards",
        className="kpi-cards-container",
    )


def create_kpi_card(icon: str, title: str, value: str, subtitle: str, subtitle_color: str = "#7f8c8d") -> html.Div:
    """Create a single KPI card component.

    Args:
        icon: Emoji icon for the card
        title: Title of the metric
        value: Main metric value to display
        subtitle: Subtitle text below the value
        subtitle_color: Color for the subtitle text (default: gray)

    Returns:
        html.Div containing the KPI card
    """
    return html.Div(
        [
            html.Div(
                f"{icon} {title}",
                className="kpi-card-title",
            ),
            html.Div(
                value,
                className="kpi-card-value",
            ),
            html.Div(
                subtitle,
                className="kpi-card-subtitle",
                style={"color": subtitle_color},
            ),
        ],
        className="kpi-card",
    )


def build_kpi_cards(
    total_arrearages: int,
    avg_monthly: float,
    unique_zips: int,
    num_utilities: int,
    total_utilities: int,
    percent_change: float,
    num_months: int,
) -> list[html.Div]:
    """Build all KPI cards for the dashboard.

    Args:
        total_arrearages: Total arrearage count
        avg_monthly: Average monthly arrearage count
        unique_zips: Number of unique zip codes
        num_utilities: Number of active/selected utilities
        total_utilities: Total number of utilities available
        percent_change: Percentage change from first to last month
        num_months: Number of months in the data

    Returns:
        List of KPI card Div components
    """
    # Determine trend color
    if percent_change < 0:
        trend_color = "#27ae60"  # Green for decrease (good)
    elif percent_change > 0:
        trend_color = "#e74c3c"  # Red for increase (bad)
    else:
        trend_color = "#7f8c8d"  # Gray for no change

    # Build trend subtitle
    trend_text = f"{percent_change:+.1f}% from first to last month" if num_months >= 2 else "N/A"

    return [
        create_kpi_card(
            icon="📈",
            title="Total Arrearages",
            value=f"{total_arrearages:,.0f}",
            subtitle=trend_text,
            subtitle_color=trend_color,
        ),
        create_kpi_card(
            icon="📊",
            title="Avg. Monthly Count",
            value=f"{avg_monthly:,.0f}",
            subtitle=f"Across {num_months} months",
        ),
        create_kpi_card(
            icon="📍",
            title="Unique Zip Codes",
            value=f"{unique_zips:,}",
            subtitle=f"In {num_utilities} utilities",
        ),
        create_kpi_card(
            icon="🏢",
            title="Active Utilities",
            value=f"{num_utilities}",
            subtitle=f"of {total_utilities} total",
        ),
    ]
