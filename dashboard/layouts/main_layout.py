"""Main layout assembly for the dashboard."""

from dash import html

from dashboard.dashboard_config import get_dataset_configs
from dashboard.layouts.filters import create_filters
from dashboard.layouts.footer import create_footer
from dashboard.layouts.header import create_header
from dashboard.layouts.tabs_dynamic import create_dynamic_tabs


def create_main_layout(
    all_utilities: list[str],
    available_months_by_year: dict[int, list[int]],
    available_years: list[int],
    start_year_default: int,
    start_month_default: int,
    end_year_default: int,
    end_month_default: int,
    month_names: list[str],
) -> html.Div:
    """Create the complete dashboard layout."""
    # Get dataset configurations
    dataset_configs = get_dataset_configs()

    return html.Div(
        [
            create_header(),
            # KPI Cards Section
            html.Div(
                id="kpi-cards-container",
                className="kpi-cards-container",
                style={"padding": "20px", "marginBottom": "20px"},
            ),
            create_filters(
                all_utilities,
                available_months_by_year,
                available_years,
                start_year_default,
                start_month_default,
                end_year_default,
                end_month_default,
                month_names,
            ),
            create_dynamic_tabs(dataset_configs),
            create_footer(),
        ],
        style={
            "padding": "0",
            "maxWidth": "1600px",
            "margin": "0 auto",
            "backgroundColor": "#f5f7fa",
            "minHeight": "100vh",
            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
        },
    )
