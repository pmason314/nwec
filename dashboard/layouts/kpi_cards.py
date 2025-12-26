"""KPI cards section with card builder components."""

from dash import html


def create_kpi_cards_section() -> html.Div:
    """Create the KPI cards section container (populated by callback)."""
    return html.Div(
        id="kpi-cards",
        className="kpi-cards-container",
    )


def create_kpi_card(
    icon: str,
    title: str,
    value: str,
    qoq_change: str,
    qoq_arrow: str,
    qoq_color: str,
    yoy_change: str,
    yoy_arrow: str,
    yoy_color: str,
    date_range: str,
) -> html.Div:
    """Create a single KPI card component with quarterly and yearly comparisons.

    Args:
        icon: Emoji icon for the card
        title: Title of the metric
        value: Main metric value to display
        qoq_change: Quarter-over-quarter change text (e.g., "+2,150 (+5.0%)")
        qoq_arrow: Arrow symbol for QoQ trend (↑ or ↓)
        qoq_color: Color for QoQ comparison
        yoy_change: Year-over-year change text (e.g., "+3,890 (+9.4%)")
        yoy_arrow: Arrow symbol for YoY trend (↑ or ↓)
        yoy_color: Color for YoY comparison
        date_range: Date range text (e.g., "Q3 2024 (Jul-Sep)")

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
                date_range,
                className="kpi-card-date",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(f"{qoq_arrow} {qoq_change}", style={"color": qoq_color, "fontWeight": "600"}),
                            html.Br(),
                            html.Span("vs Prev Qtr", style={"fontSize": "11px", "color": "#95a5a6"}),
                        ],
                        className="kpi-comparison-item",
                    ),
                    html.Div(
                        [
                            html.Span(f"{yoy_arrow} {yoy_change}", style={"color": yoy_color, "fontWeight": "600"}),
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
    total_customers_with_arrearages: int,
    total_arrearage_amount: float,
    total_disconnections: int,
    total_bill_assistance: float,
) -> list[html.Div]:
    """Build all KPI cards for the dashboard with placeholder comparisons.

    Args:
        total_customers_with_arrearages: Total number of customers with past-due balances
        total_arrearage_amount: Total dollar value of past-due balances
        total_disconnections: Total number of disconnections
        total_bill_assistance: Total bill assistance funds distributed
        date_range_text: Text describing the date range (e.g., "Jan 2020 - Dec 2024")

    Returns:
        List of KPI card Div components
    """
    # Placeholder values for comparisons (will be calculated from real data later)
    return [
        create_kpi_card(
            icon="👥",
            title="Customers with Past-Due Balances",
            value=f"{total_customers_with_arrearages:,.0f}",
            qoq_change="+2,150 (+5.0%)",
            qoq_arrow="↑",
            qoq_color="#e74c3c",  # Red for increase (bad)
            yoy_change="+3,890 (+9.4%)",
            yoy_arrow="↑",
            yoy_color="#e74c3c",
            date_range="Q3 2024 (Jul-Sep)",
        ),
        create_kpi_card(
            icon="💰",
            title="Total Past-Due Balances",
            value=f"${total_arrearage_amount:,.0f}",
            qoq_change="+$1.2M (+3.2%)",
            qoq_arrow="↑",
            qoq_color="#e74c3c",
            yoy_change="+$4.5M (+12.1%)",
            yoy_arrow="↑",
            yoy_color="#e74c3c",
            date_range="Q3 2024 (Jul-Sep)",
        ),
        create_kpi_card(
            icon="🔌",
            title="Total Disconnections",
            value=f"{total_disconnections:,.0f}",
            qoq_change="-450 (-8.5%)",
            qoq_arrow="↓",
            qoq_color="#27ae60",  # Green for decrease (good)
            yoy_change="+1,200 (+15.3%)",
            yoy_arrow="↑",
            yoy_color="#e74c3c",
            date_range="Q3 2024 (Jul-Sep)",
        ),
        create_kpi_card(
            icon="🤝",
            title="Total Bill Assistance Funds",
            value=f"${total_bill_assistance:,.0f}",
            qoq_change="+$850K (+6.2%)",
            qoq_arrow="↑",
            qoq_color="#27ae60",  # Green for increase (good)
            yoy_change="+$2.1M (+18.7%)",
            yoy_arrow="↑",
            yoy_color="#27ae60",
            date_range="Q3 2024 (Jul-Sep)",
        ),
    ]
