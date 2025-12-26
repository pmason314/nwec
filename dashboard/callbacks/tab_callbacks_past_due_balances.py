"""Callbacks for Past-Due Balances tab (Orchestration)."""

from dash import Dash

from dashboard.callbacks.callback_modules.past_due_balances_amounts import register_amounts_callbacks
from dashboard.callbacks.callback_modules.past_due_balances_counts import register_counts_callbacks
from dashboard.callbacks.callback_modules.past_due_balances_kli import register_kli_callbacks
from dashboard.callbacks.callback_modules.past_due_balances_vintage import register_vintage_callbacks
from dashboard.dashboard_config import load_dataset
from dashboard.utils import UTILITY_COLORS


def create_past_due_balances_callbacks(app: Dash, all_utilities: list[str]) -> None:
    """Create all callbacks for Past-Due Balances tab.

    This function orchestrates the registration of all callbacks by delegating
    to specialized sub-modules organized by functionality.

    Args:
        app: Dash app instance
        all_utilities: List of all utility names
    """
    # Load datasets once
    counts_data = load_dataset("arrearage_counts")
    amounts_data = load_dataset("arrearage_amounts")
    kli_amounts_data = load_dataset("kli_arrearage_amounts")

    # Colors for utilities and vintages
    colors = UTILITY_COLORS
    vintage_colors = {
        "30 Days": "#a8dadc",  # Light blue
        "60 Days": "#457b9d",  # Medium blue
        "90 Days +": "#1d3557",  # Dark blue
        "Total Arrearages": "#2c3e50",  # Very dark (for pie charts)
    }

    # Register callback groups by functionality
    register_counts_callbacks(app, counts_data, all_utilities, colors)
    register_amounts_callbacks(app, amounts_data, counts_data, all_utilities, colors)
    register_vintage_callbacks(app, amounts_data, vintage_colors)
    register_kli_callbacks(app, kli_amounts_data, vintage_colors)
