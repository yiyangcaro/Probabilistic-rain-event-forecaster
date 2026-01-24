from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import Settings
from .utils.io import ensure_dir, write_csv
from .utils.log import setup_logger
from .utils.time import parse_run_date

def _require_keys(payload: dict[str, Any], keys: list[str], context: str) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"Missing required keys in {context}: {', '.join(missing)}")

def _build_location_id(latitude: float, longitude: float) -> str:
    return f"mtl_{latitude:.4f}_{longitude:.4f}"

def _build_hourly_frame(payload: dict[str, Any], run_date: str) -> pd.DataFrame:
    _require_keys(payload, ["latitude", "longitude", "timezone", "hourly"], "root")
    hourly = payload["hourly"]
    _require_keys(
        hourly,
        [
            "time",
            "precipitation_probability",
            "precipitation",
            "temperature_2m",
            "wind_speed_10m",
        ],
        "hourly",
    )

    df = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(hourly["time"], utc=True),
            "precip_prob": hourly["precipitation_probability"],
            "precip_mm": hourly["precipitation"],
            "temp_c": hourly["temperature_2m"],
            "wind_kph": hourly["wind_speed_10m"],
        }
    )

    latitude = float(payload["latitude"])
    longitude = float(payload["longitude"])
    timezone = str(payload["timezone"])
    location_id = _build_location_id(latitude, longitude)

    df.insert(0, "run_date", run_date)
    df.insert(2, "location_id", location_id)
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["timezone"] = timezone
    df["timestamp_utc"] = df["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["date_id"] = pd.to_datetime(df["timestamp_utc"], utc=True).dt.date.astype(str)
    return df

def _build_dim_date(hourly: pd.DataFrame) -> pd.DataFrame:
    timestamps = pd.to_datetime(hourly["timestamp_utc"], utc=True)
    dates = timestamps.dt.date
    dim = pd.DataFrame({"date": dates})
    dim["date_id"] = dim["date"].astype(str)
    dim["year"] = pd.to_datetime(dim["date"]).dt.year
    dim["month"] = pd.to_datetime(dim["date"]).dt.month
    dim["day"] = pd.to_datetime(dim["date"]).dt.day
    dim["day_of_week"] = pd.to_datetime(dim["date"]).dt.dayofweek
    dim = dim.drop_duplicates().sort_values("date_id")
    dim = dim[["date_id", "date", "year", "month", "day", "day_of_week"]]
    dim["date"] = dim["date"].astype(str)
    return dim.reset_index(drop=True)

def _build_dim_location(payload: dict[str, Any]) -> pd.DataFrame:
    latitude = float(payload["latitude"])
    longitude = float(payload["longitude"])
    timezone = str(payload["timezone"])
    location_id = _build_location_id(latitude, longitude)
    return pd.DataFrame(
        [
            {
                "location_id": location_id,
                "city": "Montreal",
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
            }
        ]
    )

def _build_summary(hourly: pd.DataFrame) -> pd.DataFrame:
    hourly_dt = hourly.copy()
    hourly_dt["timestamp_utc"] = pd.to_datetime(hourly_dt["timestamp_utc"], utc=True)
    hourly_dt["date"] = hourly_dt["timestamp_utc"].dt.date.astype(str)
    summary = (
        hourly_dt.groupby("date", as_index=False)
        .agg(
            precip_mm_total=("precip_mm", "sum"),
            precip_prob_max=("precip_prob", "max"),
            temp_c_mean=("temp_c", "mean"),
            wind_kph_mean=("wind_kph", "mean"),
        )
        .sort_values("date")
    )
    return summary

def transform_forecast(
    run_date: str,
    settings: Settings,
    logger_name: str = "rain_alert.transform",
) -> dict[str, Path]:
    """Transform raw Open-Meteo JSON into reporting tables for analytics and BI."""
    run_dt = parse_run_date(run_date)
    input_path = Path(settings.data_raw_dir) / f"forecast_raw_{run_dt.isoformat()}.json"
    logger = setup_logger(logger_name)
    logger.info("transform_start", extra={"input_path": str(input_path)})

    if not input_path.exists():
        raise ValueError(f"Raw input not found: {input_path}")

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    hourly = _build_hourly_frame(payload, run_dt.isoformat())
    dim_date = _build_dim_date(hourly)
    dim_location = _build_dim_location(payload)
    summary = _build_summary(hourly)

    outputs: dict[str, Path] = {}
    processed_dir = Path(settings.data_processed_dir)
    outputs["forecast_hourly"] = write_csv(
        processed_dir / f"forecast_hourly_{run_dt.isoformat()}.csv",
        hourly,
    )
    outputs["dim_date"] = write_csv(
        processed_dir / f"dim_date_{run_dt.isoformat()}.csv",
        dim_date,
    )
    outputs["dim_location"] = write_csv(
        processed_dir / f"dim_location_{run_dt.isoformat()}.csv",
        dim_location,
    )
    outputs["forecast_summary"] = write_csv(
        processed_dir / f"forecast_summary_{run_dt.isoformat()}.csv",
        summary,
    )

    star_dir = ensure_dir(Path("data/star") / run_dt.isoformat())
    fact_hourly = hourly.copy()
    fact_hourly = fact_hourly.drop(columns=["run_date", "latitude", "longitude", "timezone"])
    dim_date_star = dim_date.copy()
    dim_location_star = dim_location.copy()
    fact_daily = summary.copy()

    outputs["star_fact_forecast_hourly"] = write_csv(
        star_dir / "fact_forecast_hourly.csv",
        fact_hourly,
    )
    outputs["star_dim_date"] = write_csv(
        star_dir / "dim_date.csv",
        dim_date_star,
    )
    outputs["star_dim_location"] = write_csv(
        star_dir / "dim_location.csv",
        dim_location_star,
    )
    outputs["star_fact_forecast_daily"] = write_csv(
        star_dir / "fact_forecast_daily.csv",
        fact_daily,
    )

    logger.info(
        "star_schema_written",
        extra={
            "star_dir": str(star_dir),
            "row_counts": {
                "fact_forecast_hourly": int(len(fact_hourly)),
                "dim_date": int(len(dim_date_star)),
                "dim_location": int(len(dim_location_star)),
                "fact_forecast_daily": int(len(fact_daily)),
            },
        },
    )

    logger.info(
        "transform_success",
        extra={"row_count": int(len(hourly)), "outputs": {k: str(v) for k, v in outputs.items()}},
    )
    return outputs
