"""
Root logger config for the EIA tool.

Configure handlers on the `eia_tool` logger. Each module calls
get_logger(__name__) to get a child; records propagate up to the
handlers here and carry the full dotted path in %(name)s.
"""

import logging
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "eia_tool.log"

_fmt = logging.Formatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_root = logging.getLogger("eia_tool")
_root.setLevel(logging.DEBUG)
_root.propagate = False  # don't leak into the true root logger

if not _root.handlers:
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(_fmt)
    _root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(_fmt)
    _root.addHandler(sh)


def get_logger(module_name: str) -> logging.Logger:
    """Return a child logger under eia_tool. Pass __name__."""
    return logging.getLogger(f"eia_tool.{module_name}")