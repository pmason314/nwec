"""Layout components for the Past-Due Balances Dashboard."""

from dashboard.layouts.filters import create_filters
from dashboard.layouts.footer import create_footer
from dashboard.layouts.header import create_header
from dashboard.layouts.kpi_cards import create_kpi_cards_section
from dashboard.layouts.tabs import create_tabs

__all__ = [
    "create_filters",
    "create_footer",
    "create_header",
    "create_kpi_cards_section",
    "create_tabs",
]
