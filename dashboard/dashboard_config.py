"""Dashboard configuration for loading all pipeline data."""

from dataclasses import dataclass

import polars as pl

from nwec.constants import PROCESSED_UTILITY_DATA
from nwec.utility_reporting.processing.standard_pipeline import load_pipelines


@dataclass
class DatasetConfig:
    """Configuration for a dataset in the dashboard."""

    name: str  # Display name for the tab
    file_name: str  # Arrow file name (without extension)
    value_column: str  # Name of the value column
    is_amount: bool = False  # True for dollar amounts, False for counts
    emoji: str = "📊"  # Emoji for the tab


def get_dataset_configs() -> list[DatasetConfig]:
    """Get all available dataset configurations from pipelines.yaml."""
    configs = []

    # Load standard pipelines from YAML
    pipelines = load_pipelines()

    # Map pipeline configurations to dashboard configs
    pipeline_mapping = {
        "disconnections": DatasetConfig(
            name="Disconnections", file_name="disconnections", value_column="Number of Disconnects", emoji="🔌"
        ),
        "disconnection_notices": DatasetConfig(
            name="Disconnection Notices",
            file_name="disconnection_notices",
            value_column="Disconnection Notice Count",
            emoji="📋",
        ),
        "payment_agreements": DatasetConfig(
            name="Payment Agreements",
            file_name="payment_agreements",
            value_column="Payment Agreement Customer Count",
            emoji="📝",
        ),
        "bill_assist": DatasetConfig(
            name="Bill Assistance", file_name="bill_assist", value_column="Bill Assist Customer Count", emoji="🤝"
        ),
        "arrearage_counts": DatasetConfig(
            name="Arrearage Counts", file_name="arrearage_counts", value_column="Arrearage Customer Count", emoji="📈"
        ),
        "uncollectible_arrears": DatasetConfig(
            name="Uncollectible Arrears",
            file_name="uncollectible_arrears",
            value_column="Uncollectible Arrearage Amount",
            is_amount=True,
            emoji="💸",
        ),
        "collection_agency_referrals": DatasetConfig(
            name="Collection Referrals",
            file_name="collection_agency_referrals",
            value_column="Collections Accounts Customer Count",
            emoji="📞",
        ),
    }

    # Add configurations for pipelines that exist
    configs = [
        pipeline_mapping[pipeline.processed_file_name]
        for pipeline in pipelines
        if pipeline.processed_file_name in pipeline_mapping
    ]

    # Add special datasets (arrearage amounts and KLI)
    if (PROCESSED_UTILITY_DATA / "arrearage_amounts.arrow").exists():
        configs.append(
            DatasetConfig(
                name="Arrearage Amounts",
                file_name="arrearage_amounts",
                value_column="Arrearage_Amount",
                is_amount=True,
                emoji="💰",
            )
        )

    if (PROCESSED_UTILITY_DATA / "kli_arrearage_amounts.arrow").exists():
        configs.append(
            DatasetConfig(
                name="KLI Arrearage Amounts",
                file_name="kli_arrearage_amounts",
                value_column="Arrearage_Amount",
                is_amount=True,
                emoji="🏠",
            )
        )

    return configs


def load_dataset(file_name: str) -> pl.DataFrame:
    """Load a dataset from the processed data directory."""
    file_path = PROCESSED_UTILITY_DATA / f"{file_name}.arrow"
    return pl.read_ipc(file_path)
