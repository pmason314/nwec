"""KPI cards section with card builder components."""

from dataclasses import dataclass

from dash import html

from dashboard.chart_config import KpiCardData


@dataclass
class MetricComparison:
    """Data structure for a metric's value and comparisons."""

    value: float
    qoq_change: str
    qoq_arrow: str
    qoq_color: str
    yoy_change: str
    yoy_arrow: str
    yoy_color: str


def create_kpi_cards_section() -> html.Div:
    """Create the KPI cards section container (populated by callback)."""
    return html.Div(
        id="kpi-cards",
        className="kpi-cards-container",
    )


def create_kpi_card(data: KpiCardData) -> html.Div:
    """Create a single KPI card component with quarterly and yearly comparisons.

    Args:
        data: KpiCardData object containing all card configuration

    Returns:
        html.Div containing the KPI card
    """
    return html.Div(
        [
            html.Div(
                f"{data.icon} {data.title}",
                className="kpi-card-title",
            ),
            html.Div(
                data.value,
                className="kpi-card-value",
            ),
            html.Div(
                data.date_range,
                className="kpi-card-date",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(
                                f"{data.qoq_arrow} {data.qoq_change}",
                                style={"color": data.qoq_color, "fontWeight": "600"},
                            ),
                            html.Br(),
                            html.Span("vs Prev Qtr", style={"fontSize": "11px", "color": "#95a5a6"}),
                        ],
                        className="kpi-comparison-item",
                    ),
                    html.Div(
                        [
                            html.Span(
                                f"{data.yoy_arrow} {data.yoy_change}",
                                style={"color": data.yoy_color, "fontWeight": "600"},
                            ),
                            html.Br(),
                            html.Span("vs Year Ago", style={"fontSize": "11px", "color": "#95a5a6"}),
                        ],
                        className="kpi-comparison-item",
                    ),
                ],
                className="kpi-comparisons",
            ),
        ],
        className="kpi-card",
    )


def build_kpi_cards(
    customers: MetricComparison,
    amount: MetricComparison,
    disconnections: MetricComparison,
    assistance: MetricComparison,
    date_range: str,
) -> list[html.Div]:
    """Build all KPI cards for the dashboard with QoQ and YoY comparisons.

    Args:
        customers: Metric comparison data for customers with past-due balances
        amount: Metric comparison data for total past-due balance amounts
        disconnections: Metric comparison data for disconnections
        assistance: Metric comparison data for bill assistance funds
        date_range: Text describing the date range (e.g., "Q3 2024 (Jul-Sep)")

    Returns:
        List of KPI card Div components
    """
    return [
        create_kpi_card(
            KpiCardData(
                icon="👥",
                title="Customers with Past-Due Balances",
                value=f"{customers.value:,.0f}",
                qoq_change=customers.qoq_change,
                qoq_arrow=customers.qoq_arrow,
                qoq_color=customers.qoq_color,
                yoy_change=customers.yoy_change,
                yoy_arrow=customers.yoy_arrow,
                yoy_color=customers.yoy_color,
                date_range=date_range,
            )
        ),
        create_kpi_card(
            KpiCardData(
                icon="💰",
                title="Total Past-Due Balances",
                value=f"${amount.value:,.0f}",
                qoq_change=amount.qoq_change,
                qoq_arrow=amount.qoq_arrow,
                qoq_color=amount.qoq_color,
                yoy_change=amount.yoy_change,
                yoy_arrow=amount.yoy_arrow,
                yoy_color=amount.yoy_color,
                date_range=date_range,
            )
        ),
        create_kpi_card(
            KpiCardData(
                icon="🔌",
                title="Total Disconnections",
                value=f"{disconnections.value:,.0f}",
                qoq_change=disconnections.qoq_change,
                qoq_arrow=disconnections.qoq_arrow,
                qoq_color=disconnections.qoq_color,
                yoy_change=disconnections.yoy_change,
                yoy_arrow=disconnections.yoy_arrow,
                yoy_color=disconnections.yoy_color,
                date_range=date_range,
            )
        ),
        create_kpi_card(
            KpiCardData(
                icon="🤝",
                title="Total Bill Assistance Funds",
                value=f"${assistance.value:,.0f}",
                qoq_change=assistance.qoq_change,
                qoq_arrow=assistance.qoq_arrow,
                qoq_color=assistance.qoq_color,
                yoy_change=assistance.yoy_change,
                yoy_arrow=assistance.yoy_arrow,
                yoy_color=assistance.yoy_color,
                date_range=date_range,
            )
        ),
    ]
