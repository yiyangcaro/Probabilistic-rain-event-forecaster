from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .config import Settings
from .extract import extract_forecast
from .transform import transform_forecast
from .validate import validate_processed
from .utils.io import write_json
from .utils.log import setup_logger
from .utils.time import now_utc, parse_run_date

def run_pipeline(
    run_date: str,
    settings: Settings,
    logger_name: str = "rain_alert.pipeline",
) -> dict[str, Any]:
    """Run extract -> transform -> validate and write a run summary."""
    run_dt = parse_run_date(run_date)
    run_date_str = run_dt.isoformat()
    logger = setup_logger(logger_name)

    stage_statuses: dict[str, str] = {}
    stage_outputs: dict[str, Any] = {}
    stage_durations: dict[str, float] = {}

    logger.info("pipeline_start", extra={"run_date": run_date_str})

    try:
        start = time.monotonic()
        logger.info("extract_start", extra={"run_date": run_date_str})
        raw_path = extract_forecast(run_date_str, settings, logger_name="rain_alert.extract")
        stage_durations["extract"] = time.monotonic() - start
        stage_statuses["extract"] = "success"
        stage_outputs["extract"] = {"raw_path": str(raw_path)}
        logger.info("extract_end", extra={"run_date": run_date_str, "duration_s": stage_durations["extract"]})
    except Exception as exc:
        stage_statuses["extract"] = "failed"
        logger.error("pipeline_failed", extra={"stage": "extract", "run_date": run_date_str}, exc_info=exc)
        raise

    try:
        start = time.monotonic()
        logger.info("transform_start", extra={"run_date": run_date_str})
        transform_outputs = transform_forecast(run_date_str, settings, logger_name="rain_alert.transform")
        stage_durations["transform"] = time.monotonic() - start
        stage_statuses["transform"] = "success"
        stage_outputs["transform"] = {k: str(v) for k, v in transform_outputs.items()}
        logger.info("transform_end", extra={"run_date": run_date_str, "duration_s": stage_durations["transform"]})
    except Exception as exc:
        stage_statuses["transform"] = "failed"
        logger.error("pipeline_failed", extra={"stage": "transform", "run_date": run_date_str}, exc_info=exc)
        raise

    try:
        start = time.monotonic()
        logger.info("validate_start", extra={"run_date": run_date_str})
        validation_result = validate_processed(run_date_str, settings, logger_name="rain_alert.validate")
        stage_durations["validate"] = time.monotonic() - start
        stage_statuses["validate"] = "success"
        stage_outputs["validate"] = {
            "status": validation_result.get("status"),
            "output_paths": {k: str(v) for k, v in validation_result.get("output_paths", {}).items()},
        }
        logger.info("validate_end", extra={"run_date": run_date_str, "duration_s": stage_durations["validate"]})
    except Exception as exc:
        stage_statuses["validate"] = "failed"
        logger.error("pipeline_failed", extra={"stage": "validate", "run_date": run_date_str}, exc_info=exc)
        raise

    overall_status = validation_result.get("status", "fail")
    run_summary = {
        "run_date": run_date_str,
        "generated_at_utc": now_utc().isoformat(),
        "status": overall_status,
        "stage_statuses": stage_statuses,
        "stage_durations_s": stage_durations,
        "paths": {
            "raw": stage_outputs.get("extract", {}).get("raw_path"),
            "processed": stage_outputs.get("transform", {}),
            "validation": stage_outputs.get("validate", {}).get("output_paths", {}),
        },
    }

    run_path = Path(settings.reports_runs_dir) / f"run_{run_date_str}.json"
    write_json(run_path, run_summary)

    logger.info("pipeline_end", extra={"run_date": run_date_str, "status": overall_status})

    return {
        "status": overall_status,
        "stage_statuses": stage_statuses,
        "stage_outputs": stage_outputs,
        "run_summary_path": run_path,
    }
