"""Collection of utility functions for the NWEC package."""

import subprocess
from pathlib import Path


def get_project_root() -> Path:
    """Return the absolute path to the project root directory."""
    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
    ).stdout.strip()
    return Path(root)
