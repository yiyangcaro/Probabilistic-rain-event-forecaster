from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    data_raw_dir: str = "data/raw"
    data_processed_dir: str = "data/processed"
    reports_validation_dir: str = "reports/validation"
    reports_exceptions_dir: str = "reports/exceptions"
    reports_runs_dir: str = "reports/runs"

    latitude: float = 45.5017
    longitude: float = -73.5673
    horizon_hours: int = 48

    @staticmethod
    def load(config_path: str) -> "Settings":
        if not config_path:
            return Settings()
        payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
        return Settings(**payload)