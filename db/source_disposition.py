"""
yearly_source_disposition database read/write operations
─────────────────────────
insert_yearly_source_disposition(records)
get_yearly_source_disposition(state, start_year, end_year)
get_yearly_source_disposition_states()
get_yearly_source_disposition_year_range()
get_yearly_state_comparison(period)
"""

import sqlite3
from pathlib import Path

from db.connection import get_connection
from utils.logger import get_logger
logger = get_logger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "eia.db"


# ── yearly_source_disposition table — writes ───────────────────────────────────────
def insert_yearly_source_disposition(records: list[dict]) -> int:
    """
    Create and update the yearly_source_disposition table.
    Returns the number of rows inserted.

    All energy values are in megawatthours (MWh).
    NULL means the value was not reported by EIA for that state/year.
    """

    def _to_int(val):
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS yearly_source_disposition (
                period                      INTEGER NOT NULL,
                state                       TEXT    NOT NULL,
                state_description           TEXT    NOT NULL,
                net_interstate_trade        INTEGER,
                total_international_exports INTEGER,
                total_international_imports INTEGER,
                total_net_generation        INTEGER,
                PRIMARY KEY (period, state)
            )
        """)

        rows = [
            (
                int(r["period"]),
                r["state"],
                r["stateDescription"],
                _to_int(r.get("net-interstate-trade")),
                _to_int(r.get("total-international-exports")),
                _to_int(r.get("total-international-imports")),
                _to_int(r.get("total-net-generation")),
            )
            for r in records
        ]

        # Only add rows with a new PRIMARY KEY (period, state)
        cur.executemany(
            """
            INSERT OR IGNORE INTO yearly_source_disposition
                (period, state, state_description,
                 net_interstate_trade, total_international_exports,
                 total_international_imports, total_net_generation)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        conn.commit()
        inserted = conn.total_changes
        conn.close()
        return inserted

    except sqlite3.Error as exc:
        logger.error("SQLite error in insert_yearly_source_disposition: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in insert_yearly_source_disposition: %s", exc)
        raise


# ── yearly_source_disposition table — reads ────────────────────────────────────────
def get_yearly_source_disposition(
    state: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[sqlite3.Row]:
    """
    Return rows from yearly_source_disposition filtered by any combination
    of state code, start year, and end year. Unfiltered if all are None.
    """
    query = "SELECT * FROM yearly_source_disposition WHERE 1=1"
    params: list = []

    if state:
        query += " AND state = ?"
        params.append(state.upper())
    if start_year is not None:
        query += " AND period >= ?"
        params.append(start_year)
    if end_year is not None:
        query += " AND period <= ?"
        params.append(end_year)

    query += " ORDER BY period DESC, state ASC"

    try:
        conn = get_connection()
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_yearly_source_disposition: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_yearly_source_disposition: %s", exc)
        raise


def get_yearly_source_disposition_states() -> list[sqlite3.Row]:
    """
    Return all (state, state_description) pairs in the table,
    ordered alphabetically by state description. Used to populate
    the state filter dropdown.
    """
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT DISTINCT state, state_description
            FROM yearly_source_disposition
            ORDER BY state_description ASC
        """).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_yearly_source_disposition_states: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_yearly_source_disposition_states: %s", exc)
        raise


def get_yearly_source_disposition_year_range() -> tuple[int, int]:
    """
    Return the earliest and latest year available in the source disposition table
    """
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT MIN(period), MAX(period)
            FROM yearly_source_disposition
        """).fetchone()
        conn.close()
        return (row[0], row[1])
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_yearly_source_disposition_year_range: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_yearly_source_disposition_year_range: %s", exc)
        raise


def get_yearly_state_comparison(period: int) -> list[sqlite3.Row]:
    """
    Return one row per U.S. state for a given year.
    Excludes aggregate U.S. totals and DC so the result contains 50 states.
    """
    try:
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT
                period,
                state,
                state_description,
                net_interstate_trade,
                total_international_exports,
                total_international_imports,
                total_net_generation
            FROM yearly_source_disposition
            WHERE period = ?
              AND state NOT IN ('US', 'DC')
            ORDER BY state ASC
            """,
            (period,),
        ).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_yearly_state_comparison: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_yearly_state_comparison: %s", exc)
        raise
