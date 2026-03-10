import json
from datetime import datetime, timezone
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

MAX_AGE_DAYS = 30


def data_is_fresh(json_file: Path, max_age_days: int = MAX_AGE_DAYS) -> bool:
    """Return True if the cached JSON exists and is less than max_age_days old."""
    if not json_file.exists():
        return False

    try:
        with open(json_file) as f:
            meta = json.load(f)
        fetched_at = datetime.fromisoformat(meta["fetched_at"])
        age = datetime.now(timezone.utc) - fetched_at
        if age.days < max_age_days:
            logger.info(
                "Cached data is %d day(s) old (fetched %s). "
                "Threshold is %d days — skipping download.",
                age.days,
                fetched_at.strftime("%Y-%m-%d %H:%M UTC"),
                max_age_days,
            )
            return True
        logger.info("Cached data is %d day(s) old — refreshing.", age.days)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.info("Could not read cache timestamp (%s) — will re-download.", exc)

    return False


def load_json_cache(json_file: Path) -> list[dict]:
    """Load and return records from the JSON cache file."""
    try:
        with open(json_file) as f:
            return json.load(f)["records"]
    except FileNotFoundError as exc:
        logger.error("JSON cache file not found: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error loading JSON cache: %s", exc)
        raise


def save_json_cache(
    json_file: Path,
    records: list[dict],
    fields: list[str],
    units: str = "megawatthours",
) -> None:
    """Write records + metadata to the JSON cache file."""
    json_file.parent.mkdir(exist_ok=True)
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "fields": fields,
        "units": units,
        "records": records,
    }

    try:
        with open(json_file, "w") as f:
            json.dump(payload, f, indent=2)
    except FileNotFoundError as exc:
        logger.error("Could not find path when saving JSON cache: %s", exc)
        raise
    except Exception as exc:
        logger.error("Unexpected error saving JSON cache to %s: %s", json_file, exc)
        raise

    size_kb = json_file.stat().st_size / 1024
    logger.info("Saved %d records to %s (%.1f KB).", len(records), json_file, size_kb)