#!/usr/bin/env python3
"""
Fetch EIA State Electricity Profiles — Generating Capacities data
(V2 API), cache the raw JSON to data/, and load it into SQLite via
the db module.

All energy values are in megawatts (MW).

Usage (from project root):
    python -m utils.fetch_yearly_generation_capacities_data          # skips if data < 30 days old
"""

import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from db.db import insert_yearly_generation_capacities, table_exists
from utils.file_utils import data_is_fresh, load_json_cache, save_json_cache
from utils.logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2"
ROUTE = "electricity/state-electricity-profiles/capability/data"

DATA_DIR = PROJECT_ROOT / "data"
DB_DIR = PROJECT_ROOT / "db"
JSON_FILE = DATA_DIR / "eia_generation_capacities.json"

FIELDS = [
    "capability",
]

START_YEAR = "1990"
END_YEAR = "2024"
BATCH_SIZE = 5000


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
        "facets[producertypeid][]": "TOT",  # filter to totals only (sum for all sectors, not broken down into utilities, independent producers)
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
        logger.info("Requesting EIA data at offset=%d …", offset)

        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
        except requests.exceptions.Timeout as exc:
            logger.error("EIA API request timed out at offset=%d: %s", offset, exc)
            raise
        except requests.exceptions.ConnectionError as exc:
            logger.error("EIA API connection error at offset=%d: %s", offset, exc)
            raise
        except requests.exceptions.HTTPError as exc:
            logger.error(
                "EIA API HTTP error at offset=%d (status %s): %s",
                offset,
                exc.response.status_code if exc.response is not None else "unknown",
                exc,
            )
            raise
        except Exception as exc:
            logger.error("Unexpected error calling EIA API at offset=%d: %s", offset, exc)
            raise

        body = resp.json()
        api_resp = body.get("response", {})

        if not api_resp:
            logger.info("Unexpected response shape from EIA API: %s", str(body)[:500])
            raise ValueError("Unexpected EIA API response shape — no 'response' key.")

        records = api_resp.get("data", [])
        total = int(api_resp.get("total", 0))
        all_records.extend(records)

        logger.info(
            "Fetched %d rows from EIA API (running total: %d / %d).",
            len(records),
            len(all_records),
            total,
        )

        if not records or len(all_records) >= total:
            break

        offset += BATCH_SIZE

    return all_records


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_eia_capacities_data() -> None:

    if not API_KEY:
        logger.error("EIA_API_KEY is not set. Add it to your .env file.")
        raise RuntimeError("EIA_API_KEY is not set.")

    if data_is_fresh(JSON_FILE):
        if not (DB_DIR / "eia.db").exists() or not table_exists("yearly_generation_capacities"):
            logger.warning("Data is fresh but table or DB is missing — rebuilding from cached JSON.")
            records = load_json_cache(JSON_FILE)
            row_count = insert_yearly_generation_capacities(records)
            logger.info("Inserted %d rows into yearly_generation_capacities.", row_count)
        return

    logger.info(
        "Fetching EIA generation capacities data (%s–%s) …", START_YEAR, END_YEAR
    )
    records = fetch_all_records()

    if not records:
        logger.info("No records returned — double-check your API key and date range.")
        raise ValueError("EIA API returned no records.")

    save_json_cache(JSON_FILE, records, FIELDS, units="megawatts")

    row_count = insert_yearly_generation_capacities(records)
    logger.info("Inserted %d rows into yearly_generation_capacities.", row_count)


if __name__ == "__main__":
    fetch_eia_capacities_data()