"""
get_connection()   — returns a sqlite3.Connection to db/eia.db
table_exists(table_name)
"""

import sqlite3
from pathlib import Path

from utils.logger import get_logger
logger = get_logger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "eia.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(table_name: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None
