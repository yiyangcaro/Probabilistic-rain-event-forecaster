from __future__ import annotations

from datetime import UTC, date, datetime

def parse_run_date(value: str) -> date:
    return date.fromisoformat(value)

def now_utc() -> datetime:
    return datetime.now(UTC)
