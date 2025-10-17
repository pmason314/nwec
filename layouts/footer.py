"""Footer component for the dashboard."""

from datetime import UTC, datetime

from dash import html


def create_footer() -> html.Div:
    """Create the footer section with data source and generation date."""
    return html.Div(
        [
            html.Div(
                [
                    html.P(
                        [
                            "Data Source: UTC Docket Case 200281 | ",
                            html.Span(
                                f"Dashboard Generated: {datetime.now(tz=UTC).strftime('%B %d, %Y')}",
                                style={"fontWeight": "500"},
                            ),
                        ],
                        style={"margin": 0, "fontSize": "14px", "color": "#7f8c8d"},
                    ),
                ],
                style={"textAlign": "center"},
            ),
        ],
        style={
            "padding": "20px",
            "backgroundColor": "#f8f9fa",
            "borderTop": "1px solid #e1e8ed",
            "marginTop": "30px",
        },
    )
