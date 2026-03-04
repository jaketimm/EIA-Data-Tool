"""
Shared logger configuration for the EIA tool.

All modules obtain a logger via get_logger(__name__). Output is written
to logs/eia_tool.log and echoed to the console. Handlers are attached to
a single parent logger so configuration only runs once regardless of
import order.
"""

import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "eia_tool.log"


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the 'eia_tool' namespace."""
    root = logging.getLogger("eia_tool")

    # Configure once — guard against duplicate handlers on re-import.
    if not root.handlers:
        LOG_DIR.mkdir(exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

        # Remove this handler if you want file-only logging.
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

        root.setLevel(logging.INFO)

    return logging.getLogger(f"eia_tool.{name}")