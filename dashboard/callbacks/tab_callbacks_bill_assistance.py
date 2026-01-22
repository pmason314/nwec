"""Callbacks for Bill Assistance tab."""

from dash import Dash

from dashboard.callbacks.callback_modules.bill_assistance_enrollment import register_enrollment_callbacks
from dashboard.callbacks.callback_modules.bill_assistance_funding import register_funding_callbacks
from dashboard.callbacks.callback_modules.bill_assistance_payment_plans import register_payment_plans_callbacks
from dashboard.dashboard_config import load_dataset


def create_bill_assistance_callbacks(app: Dash) -> None:
    """Register all Bill Assistance callbacks.

    Args:
        app: Dash app instance
    """
    # Load data
    df_bill_assist = load_dataset("bill_assist")
    df_payment_arr = load_dataset("payment_agreements")
    df_liheap = load_dataset("assistance_liheap")
    df_utility_assist = load_dataset("assistance_utility")

    # Utility colors
    utility_colors = {
        "Avista": "#003768",
        "PSE": "#2596be",
        "PAC": "#e90028",
        "CNG": "#6c757d",
        "NWN": "#54b266",
    }

    # Register callbacks for each section
    register_enrollment_callbacks(app, df_bill_assist, utility_colors)
    register_payment_plans_callbacks(app, df_payment_arr, utility_colors)
    register_funding_callbacks(app, df_liheap, df_utility_assist, utility_colors)
