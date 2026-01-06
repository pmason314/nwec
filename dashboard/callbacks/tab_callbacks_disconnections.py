"""Callbacks for Disconnections tab visualizations."""

from dash import Dash

from dashboard.callbacks.callback_modules.disconnections_actual import register_actual_disconnections_callbacks
from dashboard.callbacks.callback_modules.disconnections_notices import register_disconnection_notices_callbacks
from dashboard.dashboard_config import load_dataset
from dashboard.utils import UTILITY_COLORS


def create_disconnections_callbacks(app: Dash, all_utilities: list[str]) -> None:
    """Create all callbacks for Disconnections tab.

    Args:
        app: Dash app instance
        all_utilities: List of all utility names
    """
    # Load datasets
    disconnections_data = load_dataset("disconnections")
    disconnection_notices_data = load_dataset("disconnection_notices")

    # Colors for utilities
    colors = UTILITY_COLORS

    # Register callbacks for each section
    register_actual_disconnections_callbacks(app, disconnections_data, all_utilities, colors)
    register_disconnection_notices_callbacks(app, disconnection_notices_data, all_utilities, colors)
