from __future__ import annotations

from pathlib import Path
from typing import Any

import requests

from .config import Settings
from .utils.io import write_json
from .utils.log import setup_logger
from .utils.time import parse_run_date

DEFAULT_HOURLY_FIELDS = [
    "precipitation_probability",
    "precipitation",
    "temperature_2m",
    "wind_speed_10m",
]

def build_open_meteo_params(settings: Settings) -> dict[str, Any]:
    return {
        "latitude": settings.latitude,
        "longitude": settings.longitude,
        "hourly": ",".join(DEFAULT_HOURLY_FIELDS),
        "forecast_hours": settings.horizon_hours,
        "timezone": "UTC",
    }

def extract_forecast(
    run_date: str,
    settings: Settings,
    *,
    session: requests.Session | None = None,
    logger_name: str = "rain_alert.extract",
) -> Path:
    """Fetch Open-Meteo hourly forecast data and persist the raw JSON payload."""
    run_dt = parse_run_date(run_date)
    logger = setup_logger(logger_name)

    url = "https://api.open-meteo.com/v1/forecast"
    params = build_open_meteo_params(settings)
    logger.info("request_start", extra={"url": url, "params": params})

    sess = session or requests.Session()
    try:
        response = sess.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("request_failed", exc_info=exc)
        raise RuntimeError(f"Open-Meteo request failed: {exc}") from exc

    payload = response.json()
    output_path = Path(settings.data_raw_dir) / f"forecast_raw_{run_dt.isoformat()}.json"
    write_json(output_path, payload)
    logger.info("request_success", extra={"output_path": str(output_path)})
    return output_path
