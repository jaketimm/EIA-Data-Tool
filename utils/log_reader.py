"""
Parse logs/eia_tool.log into structured records for the /logs view.

Relies on the format set in utils/logger.py:
    YYYY-MM-DD HH:MM:SS - eia_tool.<module> - LEVEL - message

Lines that don't match the pattern are treated as continuations of the
previous record (covers tracebacks from logger.exception()).
"""

import re
from pathlib import Path

LOG_FILE = Path(__file__).resolve().parent.parent / "logs" / "eia_tool.log"

_LINE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - "
    r"(?P<name>eia_tool[\w.]*) - "
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL) - "
    r"(?P<msg>.*)$"
)


def read_log_records(
    limit: int = 500,
    level: str | None = None,
    search: str | None = None,
) -> list[dict]:
    """
    Return up to `limit` records, newest first, optionally filtered by
    exact level match and/or case-insensitive substring search across
    module name + message.
    """
    if not LOG_FILE.exists():
        return []

    records: list[dict] = []
    with open(LOG_FILE, encoding="utf-8") as f:
        for line in f:
            m = _LINE.match(line)
            if m:
                records.append(m.groupdict())
            elif records:
                # Continuation line — fold into previous record.
                records[-1]["msg"] += "\n" + line.rstrip("\n")

    if level:
        records = [r for r in records if r["level"] == level]
    if search:
        q = search.lower()
        records = [
            r for r in records
            if q in r["msg"].lower() or q in r["name"].lower()
        ]

    # Tail to limit, then reverse → newest first.
    return records[-limit:][::-1]