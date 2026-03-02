"""
Database access layer.

Connection helper
─────────────────
get_connection()   — returns a sqlite3.Connection to db/eia.db

yearly_source_disposition
─────────────────────────
insert_yearly_source_disposition(records)
get_yearly_source_disposition(state, start_year, end_year)
get_yearly_source_disposition_states()
get_yearly_source_disposition_year_range()
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "eia.db"


# ── Connection ────────────────────────────────────────────────────────────────
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # rows behave like dicts
    return conn


# ── yearly_source_disposition — writes ───────────────────────────────────────
def insert_yearly_source_disposition(records: list[dict]) -> int:
    """
    Drop, recreate, and populate the yearly_source_disposition table.
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

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS yearly_source_disposition")
    cur.execute("""
        CREATE TABLE yearly_source_disposition (
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

    cur.executemany(
        """
        INSERT INTO yearly_source_disposition
            (period, state, state_description,
             net_interstate_trade, total_international_exports,
             total_international_imports, total_net_generation)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    conn.close()
    return len(rows)


# ── yearly_source_disposition — reads ────────────────────────────────────────
def get_yearly_source_disposition(
    state: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[sqlite3.Row]:
    """
    Return rows from yearly_source_disposition filtered by any combination
    of state code, start year, and end year. Unfiltered if all are None.
    Results are ordered period DESC, state ASC.
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

    conn = get_connection()
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def get_yearly_source_disposition_states() -> list[sqlite3.Row]:
    """
    Return all (state, state_description) pairs in the table,
    ordered alphabetically by state description. Used to populate
    the state filter dropdown.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT state, state_description
        FROM yearly_source_disposition
        ORDER BY state_description ASC
    """).fetchall()
    conn.close()
    return rows


def get_yearly_source_disposition_year_range() -> tuple[int, int]:
    """
    Return the (min_year, max_year) present in yearly_source_disposition.
    Used to set sensible bounds on the year-range filter inputs.
    """
    conn = get_connection()
    row = conn.execute("""
        SELECT MIN(period), MAX(period)
        FROM yearly_source_disposition
    """).fetchone()
    conn.close()
    return (row[0], row[1])