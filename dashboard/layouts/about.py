"""About tab content for the dashboard."""

from dash import dcc, html


def create_about_tab() -> dcc.Tab:
    """Create the static About tab as the first tab."""
    return dcc.Tab(
        label="About",
        value="about-tab",
        style={
            "padding": "12px 30px",
            "fontWeight": "500",
            "fontSize": "16px",
        },
        selected_style={
            "padding": "12px 30px",
            "fontWeight": "700",
            "fontSize": "16px",
            "borderTop": "4px solid #156570",
            "backgroundColor": "white",
        },
        children=[
            html.Div(
                [
                    html.H2(
                        "About the Energy Affordability Dashboard",
                        style={
                            "marginTop": 0,
                            "marginBottom": "10px",
                            "color": "#2c3e50",
                            "fontSize": "24px",
                            "fontWeight": "700",
                        },
                    ),
                    html.P(
                        (
                            "As energy bills are increasing in Washington State and across the country, it's "
                            "increasingly important for utilities to report what is happening and for the public "
                            "to critically analyze the trends. The Energy Affordability Dashboard is a tool that "
                            "visually and numerically tells the story of how Washington's residential energy utility "
                            "customers are experiencing past-due balances, disconnections, bill assistance, and "
                            "collections. With this tool you can explore geographic patterns, identify trends over "
                            "time and between utilities, download data and graphics, and more."
                        ),
                        style={"color": "#34495e", "fontSize": "16px", "lineHeight": "1.6"},
                    ),
                    html.H3(
                        "About the Analysis",
                        style={
                            "marginTop": "20px",
                            "marginBottom": "8px",
                            "color": "#2c3e50",
                            "fontSize": "18px",
                            "fontWeight": "600",
                        },
                    ),
                    html.P(
                        [
                            "Data in this dashboard comes from monthly and quarterly reports that utilities file with"
                            " the Washington Utilities & Transportation Commission in ",
                            html.A(
                                "Docket U-200281",
                                href="https://www.utc.wa.gov/casedocket/2020/200281/docsets",
                                target="_blank",
                                style={"color": "#156570", "textDecoration": "none", "fontWeight": "600"},
                            ),
                            ".",
                        ],
                        style={"color": "#34495e", "fontSize": "16px", "lineHeight": "1.6"},
                    ),
                    html.P(
                        [
                            "For additional information on utility disconnections data and policies in Washington State"
                            " and nationwide, see the ",
                            html.A(
                                "Indiana University Utility Disconnections Dashboard",
                                href="https://energyjustice.indiana.edu/disconnection-dashboard/index.html",
                                target="_blank",
                                style={"color": "#156570", "textDecoration": "none", "fontWeight": "600"},
                            ),
                            ".",
                        ],
                        style={"color": "#34495e", "fontSize": "16px", "lineHeight": "1.6"},
                    ),
                    html.P(
                        "This analysis was prepared by the NW Energy Coalition and this dashboard was prepared by Peter"
                        " Mason.",
                        style={"color": "#34495e", "fontSize": "16px", "lineHeight": "1.6"},
                    ),
                ],
                style={
                    "padding": "25px",
                    "backgroundColor": "white",
                    "borderRadius": "8px",
                    "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
                    "border": "1px solid #e1e8ed",
                },
            )
        ],
    )
