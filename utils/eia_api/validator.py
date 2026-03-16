from utils.logger import get_logger

logger = get_logger(__name__)


def detect_schema_drift(EXPECTED_FIELDS, records: list[dict]) -> bool:
    """Check the data schema. Return True if the data passes the schema check"""
    if not records:
        return False

    record_keys = set(records[0].keys())

    missing = EXPECTED_FIELDS - record_keys
    extra = record_keys - EXPECTED_FIELDS

    if missing or extra:
        logger.warning(
            "Schema drift detected. Missing=%s Extra=%s",
            missing,
            extra,
        )

        return False
    
    return True