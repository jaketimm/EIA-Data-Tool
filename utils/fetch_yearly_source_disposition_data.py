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
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from db.db import insert_yearly_source_disposition, table_exists
from utils.logger import get_logger
logger = get_logger(__name__)

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
            logger.info(
                "Cached data is %d day(s) old (fetched %s). "
                "Threshold is %d days — skipping download.",
                age.days,
                fetched_at.strftime("%Y-%m-%d %H:%M UTC"),
                MAX_AGE_DAYS,
            )
            return True
        logger.info("Cached data is %d day(s) old — refreshing.", age.days)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.info("Could not read cache timestamp (%s) — will re-download.", exc)

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

    try:
        with open(JSON_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except FileNotFoundError as exc:
        logger.error("Could not find path when saving JSON cache: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error saving JSON cache to %s: %s", JSON_FILE, exc)
        raise

    size_kb = JSON_FILE.stat().st_size / 1024
    logger.info("Saved %d records to %s (%.1f KB).", len(records), JSON_FILE, size_kb)


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_eia_source_data():

    if not API_KEY:
        logger.error("EIA_API_KEY is not set. Add it to your .env file.")
        raise RuntimeError("EIA_API_KEY is not set.")

    if data_is_fresh():
        if not (DB_DIR / "eia.db").exists() or not table_exists("yearly_source_disposition"):
            logger.warning("Data is fresh but table or DB is missing — rebuilding from cached JSON.")
            try:
                with open(JSON_FILE) as f:
                    records = json.load(f)["records"]
            except FileNotFoundError as exc:
                logger.error("JSON cache file not found when rebuilding DB: %s", exc)
                raise
            except Exception as exc:
                logger.error("Unexpected error loading JSON cache for DB rebuild: %s", exc)
                raise
            row_count = insert_yearly_source_disposition(records)
            logger.info("Inserted %d rows into yearly_source_disposition.", row_count)
        return

    logger.info(
        "Fetching EIA source & disposition data (%s–%s) …", START_YEAR, END_YEAR
    )
    records = fetch_all_records()

    if not records:
        logger.info(
            "No records returned — double-check your API key and date range."
        )
        raise ValueError("EIA API returned no records.")

    save_json(records)

    row_count = insert_yearly_source_disposition(records)
    logger.info("Inserted %d rows into yearly_source_disposition.", row_count)


if __name__ == "__main__":
    fetch_eia_source_data()