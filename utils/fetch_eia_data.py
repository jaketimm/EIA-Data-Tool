#!/usr/bin/env python3
"""
Fetch EIA State Electricity Profiles — Source & Disposition data
(V2 API), cache the raw JSON, and load into a SQLite database.

All energy values are stored in megawatthours (MWh).

Usage (from project root):
    python -m utils.fetch_eia_data           # skips if data < 30 days old
    python -m utils.fetch_eia_data --force   # always re-download
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Paths (relative to project root) ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
JSON_FILE = DATA_DIR / "eia_source_disposition.json"
DB_FILE = DATA_DIR / "eia.db"

# ── API config ────────────────────────────────────────────────────────────────
API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2"
ROUTE = "electricity/state-electricity-profiles/source-disposition/data"

FIELDS = [
    "net-interstate-trade",
    "total-international-exports",
    "total-international-imports",
    "total-net-generation",
]

START_YEAR = "1990"
END_YEAR = "2024"
BATCH_SIZE = 5000
MAX_AGE_DAYS = 30


# ── Freshness check ──────────────────────────────────────────────────────────
def data_is_fresh() -> bool:
    """Return True if the cached JSON exists and is less than MAX_AGE_DAYS old."""
    if not JSON_FILE.exists():
        return False

    try:
        with open(JSON_FILE) as f:
            meta = json.load(f)
        fetched_at = datetime.fromisoformat(meta["fetched_at"])
        age = datetime.now(timezone.utc) - fetched_at
        if age.days < MAX_AGE_DAYS:
            print(
                f"Cached data is {age.days} day(s) old "
                f"(fetched {fetched_at.strftime('%Y-%m-%d %H:%M UTC')}). "
                f"Threshold is {MAX_AGE_DAYS} days — skipping download."
            )
            return True
        print(f"Cached data is {age.days} day(s) old — refreshing.")
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        print(f"Could not read cache timestamp ({exc}) — will re-download.")

    return False


# ── API helpers ───────────────────────────────────────────────────────────────
def build_params(offset: int = 0) -> dict:
    """Flatten nested params into the query-string format the V2 API expects."""
    params = {
        "api_key": API_KEY,
        "frequency": "annual",
        "start": START_YEAR,
        "end": END_YEAR,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": BATCH_SIZE,
    }
    for i, field in enumerate(FIELDS):
        params[f"data[{i}]"] = field
    return params


def fetch_all_records() -> list[dict]:
    """Page through the API until every record has been collected."""
    url = f"{BASE_URL}/{ROUTE}/"
    all_records: list[dict] = []
    offset = 0

    while True:
        params = build_params(offset)
        print(f"  → requesting offset={offset} …")

        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        api_resp = body.get("response", {})
        if not api_resp:
            print("Unexpected response shape — dumping body:")
            print(json.dumps(body, indent=2)[:2000])
            sys.exit(1)

        records = api_resp.get("data", [])
        total = int(api_resp.get("total", 0))
        all_records.extend(records)

        print(f"    fetched {len(records)} rows  (running total: {len(all_records)} / {total})")

        if not records or len(all_records) >= total:
            break

        offset += BATCH_SIZE

    return all_records


def save_json(records: list[dict]) -> None:
    """Write records + metadata to the JSON cache file."""
    DATA_DIR.mkdir(exist_ok=True)
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "fields": FIELDS,
        "units": "megawatthours",
        "records": records,
    }
    with open(JSON_FILE, "w") as f:
        json.dump(payload, f, indent=2)

    size_kb = JSON_FILE.stat().st_size / 1024
    print(f"Saved {len(records)} records to {JSON_FILE}  ({size_kb:.1f} KB)")


# ── SQLite ────────────────────────────────────────────────────────────────────
def _to_int(val) -> int | None:
    """Coerce API string values ("6690506", "0", null) to int or None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def load_db(records: list[dict]) -> None:
    """
    (Re)create the yearly_source_disposition table and insert all records.

    Schema mirrors the API field names with hyphens → underscores.
    All energy columns are INTEGER (megawatthours). NULL means the
    value was not reported by EIA.
    """
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
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

    size_kb = DB_FILE.stat().st_size / 1024
    print(f"Loaded {len(rows)} records into {DB_FILE}  ({size_kb:.1f} KB)")


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(records: list[dict]) -> None:
    states = sorted({r.get("stateDescription", "?") for r in records})
    years = sorted({r.get("period", "?") for r in records})
    print(f"\n{'─' * 60}")
    print(f"  Records : {len(records)}")
    print(f"  States  : {len(states)}")
    print(f"  Years   : {years[0]} – {years[-1]}")
    print(f"  Units   : megawatthours (MWh)")
    print(f"{'─' * 60}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    force = "--force" in sys.argv

    if not API_KEY:
        print("ERROR: EIA_API_KEY is not set. Add it to your .env file.")
        sys.exit(1)

    if not force and data_is_fresh():
        # Even though we skip the download, make sure the DB exists.
        # If the JSON is cached but someone deleted the DB, rebuild it.
        if not DB_FILE.exists():
            print("DB missing — rebuilding from cached JSON …")
            with open(JSON_FILE) as f:
                records = json.load(f)["records"]
            load_db(records)
            print_summary(records)
        return

    print(f"Fetching EIA source & disposition data ({START_YEAR}–{END_YEAR}) …\n")

    records = fetch_all_records()

    if not records:
        print("\nNo records returned — double-check your API key and date range.")
        sys.exit(1)

    save_json(records)
    load_db(records)
    print_summary(records)


if __name__ == "__main__":
    main()