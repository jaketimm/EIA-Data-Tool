"""
yearly_generation_capacities database read/write operations
─────────────────────────
insert_yearly_generation_capacities(records)
get_generation_capacities_state_list
get_generation_capacities_year_range(state)
get_generation_capacities_by_year(period)
get_generation_capacities_for_state(state, start_year, end_year)
"""

import sqlite3
from pathlib import Path

from db.connection import get_connection
from utils.logger import get_logger
logger = get_logger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "eia.db"


# ── yearly_generation_capacities table — writes ───────────────────────────────────────
def insert_yearly_generation_capacities(records: list[dict]) -> int:
    """
    Create and update the yearly_generation_capacities table.
    Returns the number of rows inserted.

    All energy values are in megawatts (MW).
    """

    def _to_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS yearly_generation_capacities (
                period                      INTEGER NOT NULL,
                state                       TEXT    NOT NULL,
                state_description           TEXT    NOT NULL,
                energy_source_id        TEXT,
                energy_source_description TEXT,
                capability        REAL,
                PRIMARY KEY (period, state, energy_source_id)
            )
        """)

        rows = [
            (
                int(r["period"]),
                r["stateId"],
                r["stateDescription"],
                r["energysourceid"],
                r["energySourceDescription"],
                _to_float(r.get("capability")),
            )
            for r in records
        ]

        # Only add rows with a new PRIMARY KEY (period, state, energy_source_id)
        cur.executemany(
            """
            INSERT OR IGNORE INTO yearly_generation_capacities
                (period, state, state_description,
                 energy_source_id, energy_source_description,
                 capability)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        conn.commit()
        inserted = conn.total_changes
        conn.close()
        return inserted

    except sqlite3.Error as exc:
        logger.error("SQLite error in insert_yearly_generation_capacities: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in insert_yearly_generation_capacities: %s", exc)
        raise


# ── yearly_generation_capacities table — reads ───────────────────────────────────────
def get_generation_capacities_state_list() -> list[sqlite3.Row]:
    """
    Return all state codes and descriptions available for generation capacities.
    Used to populate the state filter dropdown.
    """
    try:
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT DISTINCT state, state_description
            FROM yearly_generation_capacities
            WHERE state NOT IN ('US', 'DC')
            ORDER BY state_description ASC
            """
        ).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_generation_capacities_state_list: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_generation_capacities_state_list: %s", exc)
        raise


def get_generation_capacities_year_range(state: str | None = None) -> tuple[int, int]:
    """
    Return the earliest and latest year available in the generation capacities
    table, optionally scoped to a single state.
    """
    query = """
        SELECT MIN(period), MAX(period)
        FROM yearly_generation_capacities
        WHERE state NOT IN ('US', 'DC')
    """
    params: list = []

    if state:
        query += " AND state = ?"
        params.append(state.upper())

    try:
        conn = get_connection()
        row = conn.execute(query, params).fetchone()
        conn.close()

        if row is None or row[0] is None or row[1] is None:
            raise ValueError("No generation capacities data available.")

        return (int(row[0]), int(row[1]))
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_generation_capacities_year_range: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_generation_capacities_year_range: %s", exc)
        raise


def get_generation_capacities_by_year(period: int) -> list[sqlite3.Row]:
    """
    Return all state-level generation capacity rows for the supplied year,
    excluding the national and DC aggregates.
    """
    try:
        conn = get_connection()
        rows = conn.execute(
            """
            SELECT
                state,
                state_description,
                energy_source_id,
                energy_source_description,
                capability
            FROM yearly_generation_capacities
            WHERE period = ?
              AND state NOT IN ('US', 'DC')
              AND UPPER(COALESCE(energy_source_id, '')) <> 'ALL'
              AND LOWER(COALESCE(energy_source_description, '')) NOT IN ('all', 'all?')
            ORDER BY state_description ASC, energy_source_description ASC
            """,
            (period,),
        ).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_generation_capacities_by_year: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_generation_capacities_by_year: %s", exc)
        raise


def get_generation_capacities_for_state(state: str, start_year: int | None = None,
    end_year: int | None = None,) -> list[sqlite3.Row]:
    """
    Return capacity breakdown rows for a given state across the requested year range.
    """
    query = """
        SELECT
            period,
            state,
            state_description,
            energy_source_id,
            energy_source_description,
            capability
        FROM yearly_generation_capacities
        WHERE state = ?
          AND state NOT IN ('US', 'DC')
          AND UPPER(COALESCE(energy_source_id, '')) <> 'ALL'
          AND LOWER(COALESCE(energy_source_description, '')) NOT IN ('all', 'all?')
    """
    params: list = [state.upper()]

    if start_year is not None:
        query += " AND period >= ?"
        params.append(start_year)
    if end_year is not None:
        query += " AND period <= ?"
        params.append(end_year)

    query += " ORDER BY period ASC, energy_source_description ASC"

    try:
        conn = get_connection()
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return rows
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_generation_capacities_for_state: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error in get_generation_capacities_for_state: %s", exc)
        raise


