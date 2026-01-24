from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable

STAR_ROOT = Path("data/star")

def _parse_date_folder(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.name)
    except ValueError:
        return None

def _iter_star_dates() -> Iterable[Path]:
    if not STAR_ROOT.exists():
        return []
    return [p for p in STAR_ROOT.iterdir() if p.is_dir()]

def latest_star_folder() -> Path | None:
    candidates = []
    for path in _iter_star_dates():
        parsed = _parse_date_folder(path)
        if parsed:
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]

def print_latest_star_paths() -> None:
    latest = latest_star_folder()
    if latest is None:
        print("No star schema folders found in data/star/")
        return
    files = [
        latest / "fact_forecast_hourly.csv",
        latest / "fact_forecast_daily.csv",
        latest / "dim_date.csv",
        latest / "dim_location.csv",
    ]
    print(f"latest_star_folder: {latest}")
    for path in files:
        print(path)
