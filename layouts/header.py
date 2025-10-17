"""Header component for the dashboard."""

from datetime import UTC, datetime

from dash import html


def create_header() -> html.Div:
    """Create the header section with title and last updated date."""
    return html.Div(
        [
            html.Div(
                [
                    html.H1(
                        "Arrearage Counts Dashboard",
                        style={
                            "color": "white",
                            "margin": 0,
                            "fontSize": "32px",
                            "fontWeight": "600",
                        },
                    ),
                    html.P(
                        "Utility arrearage trends",
                        style={
                            "color": "rgba(255, 255, 255, 0.9)",
                            "margin": "8px 0 0 0",
                            "fontSize": "16px",
                        },
                    ),
                ],
                style={"flex": "1"},
            ),
            html.Div(
                [
                    html.Div(
                        id="last-updated",
                        children=f"Last Updated: {datetime.now(tz=UTC).strftime('%B %d, %Y')}",
                        style={
                            "color": "rgba(255, 255, 255, 0.8)",
                            "fontSize": "14px",
                            "textAlign": "right",
                        },
                    ),
                ],
            ),
        ],
        style={
            "background": "linear-gradient(135deg, #156570 0%, #0d4b52 100%)",
            "padding": "30px 40px",
            "marginBottom": "30px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
        },
    )
