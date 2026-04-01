def validate_year_format(year_str: str) -> bool:
    """Validate that year_str is a 4-digit string representing a year."""
    return isinstance(year_str, str) and year_str.isdigit() and len(year_str) == 4

def validate_period(period_str: str) -> int:
    """Convert and validate period string; raise ValueError if invalid format."""
    if not validate_year_format(period_str):
        raise ValueError(f"Invalid year format: {period_str} (expected YYYY)")
    return int(period_str)