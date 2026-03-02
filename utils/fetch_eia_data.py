#!/usr/bin/env python3
"""
Fetch EIA State Electricity Profiles — Source & Disposition data
(V2 API) and save the raw JSON to data/ for inspection.

Usage (from project root):
    python -m utils.fetch_eia_data
    — or —
    python utils/fetch_eia_data.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# Resolve paths relative to project root regardless of where the script is invoked
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2"
ROUTE = "electricity/state-electricity-profiles/source-disposition/data"

DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "eia_source_disposition.json"

FIELDS = [
    "net-interstate-trade",
    "total-international-exports",
    "total-international-imports",
    "total-net-generation",
]

START_YEAR = "1990"
END_YEAR = "2024"
BATCH_SIZE = 5000


# ── Helpers ───────────────────────────────────────────────────────────────────
def build_params(offset: int = 0) -> dict:
    """
    Flatten the nested X-Params structure into the query-string format
    that the EIA V2 API expects (e.g. data[0]=..., sort[0][column]=...).
    """
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
        print(f"  → requesting offset={offset} ...")

        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        # The V2 API nests everything under "response"
        api_resp = body.get("response", {})
        if not api_resp:
            print("Unexpected response shape — dumping body:")
            print(json.dumps(body, indent=2)[:2000])
            sys.exit(1)

        records = api_resp.get("data", [])
        total = int(api_resp.get("total", 0))
        all_records.extend(records)

        print(f"    fetched {len(records)} rows  (running total: {len(all_records)} / {total})")

        # Stop when we've collected everything or the API returns nothing
        if not records or len(all_records) >= total:
            break

        offset += BATCH_SIZE

    return all_records


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not API_KEY:
        print("ERROR: EIA_API_KEY is not set. Add it to your .env file.")
        sys.exit(1)

    print(f"Fetching EIA source & disposition data ({START_YEAR}–{END_YEAR}) …\n")

    records = fetch_all_records()

    if not records:
        print("\nNo records returned — double-check your API key and date range.")
        sys.exit(1)

    # ── Quick summary ─────────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"Total records : {len(records)}")
    print(f"Record keys   : {list(records[0].keys())}")

    # Show a few unique states so the user can sanity-check
    states = sorted({r.get("stateDescription", r.get("statedescription", "?")) for r in records})
    print(f"States found  : {len(states)}")

    years = sorted({r.get("period", "?") for r in records})
    print(f"Year range    : {years[0]} – {years[-1]}")

    print(f"\nSample record:\n{json.dumps(records[0], indent=2)}")

    # ── Save ──────────────────────────────────────────────────────────────
    DATA_DIR.mkdir(exist_ok=True)

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "fields": FIELDS,
        "records": records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"\nSaved {len(records)} records to {OUTPUT_FILE}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()