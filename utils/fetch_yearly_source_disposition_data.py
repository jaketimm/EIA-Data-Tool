#!/usr/bin/env python3
"""
Fetch EIA State Electricity Profiles — Source & Disposition data
(V2 API), cache the raw JSON to data/, and load it into SQLite via
the db module.

All energy values are in megawatthours (MWh).

Usage (from project root):
    python -m utils.fetch_yearly_source_disposition_data          # skips if data < 30 days old
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from db.db import insert_yearly_source_disposition

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2"
ROUTE = "electricity/state-electricity-profiles/source-disposition/data"

DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = PROJECT_ROOT / "db"
JSON_FILE = DATA_DIR / "eia_source_disposition.json"

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


# ── Freshness check ───────────────────────────────────────────────────────────
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


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_eia_source_data():

    if not API_KEY:
        print("ERROR: EIA_API_KEY is not set. Add it to your .env file.")
        sys.exit(1)

    if data_is_fresh():
        if not (DB_DIR / "eia.db").exists():
            print("DB missing — rebuilding from cached JSON …")
            with open(JSON_FILE) as f:
                records = json.load(f)["records"]
            row_count = insert_yearly_source_disposition(records)
            print(f"Inserted {row_count} rows into yearly_source_disposition.")
        return

    print(f"Fetching EIA source & disposition data ({START_YEAR}–{END_YEAR}) …\n")
    records = fetch_all_records()

    if not records:
        print("\nNo records returned — double-check your API key and date range.")
        sys.exit(1)

    save_json(records)

    row_count = insert_yearly_source_disposition(records)
    print(f"Inserted {row_count} rows into yearly_source_disposition.")


if __name__ == "__main__":
    fetch_eia_source_data()