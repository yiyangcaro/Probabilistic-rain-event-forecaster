from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .config import Settings
from .utils.io import write_csv, write_json
from .utils.log import setup_logger
from .utils.time import now_utc, parse_run_date

EXCEPTION_COLUMNS = [
    "run_date",
    "check_name",
    "severity",
    "row_selector",
    "details",
    "n_rows_affected",
    "sample",
]

def _load_processed(run_date: str, settings: Settings) -> pd.DataFrame:
    run_dt = parse_run_date(run_date)
    path = Path(settings.data_processed_dir) / f"forecast_hourly_{run_dt.isoformat()}.csv"
    if not path.exists():
        raise ValueError(f"Processed input not found: {path}")
    return pd.read_csv(path)

def _load_raw_row_count(run_date: str, settings: Settings) -> int:
    run_dt = parse_run_date(run_date)
    raw_path = Path(settings.data_raw_dir) / f"forecast_raw_{run_dt.isoformat()}.json"
    if not raw_path.exists():
        raise ValueError(f"Raw input not found: {raw_path}")
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    hourly = payload.get("hourly", {})
    times = hourly.get("time")
    if times is None:
        raise ValueError("Raw JSON missing hourly.time for reconciliation_count check")
    return len(times)

def _sample_rows(df: pd.DataFrame, n: int = 3) -> str:
    if df.empty:
        return "[]"
    return json.dumps(df.head(n).to_dict(orient="records"), ensure_ascii=True)

def _sample_values(values: list[Any], n: int = 3) -> str:
    return json.dumps(values[:n], ensure_ascii=True)

def validate_processed(
    run_date: str,
    settings: Settings,
    logger_name: str = "rain_alert.validate",
) -> dict[str, Any]:
    """Validate processed forecast outputs and write validation + exceptions reports."""
    run_dt = parse_run_date(run_date)
    run_date_str = run_dt.isoformat()
    logger = setup_logger(logger_name)

    processed_path = Path(settings.data_processed_dir) / f"forecast_hourly_{run_date_str}.csv"
    validation_path = Path(settings.reports_validation_dir) / f"validation_{run_date_str}.json"
    exceptions_path = Path(settings.reports_exceptions_dir) / f"exceptions_{run_date_str}.csv"

    logger.info("validate_start", extra={"input_path": str(processed_path)})

    df = _load_processed(run_date_str, settings)
    row_count = int(len(df))

    checks: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []

    def add_check(
        name: str,
        passed: bool,
        severity: str,
        message: str,
        metrics: dict[str, Any],
        *,
        row_selector: str = "all",
        n_rows_affected: int = 0,
        sample: str = "[]",
    ) -> None:
        checks.append(
            {
                "name": name,
                "passed": passed,
                "severity": severity,
                "message": message,
                "metrics": metrics,
            }
        )
        if not passed:
            exceptions.append(
                {
                    "run_date": run_date_str,
                    "check_name": name,
                    "severity": severity,
                    "row_selector": row_selector,
                    "details": message,
                    "n_rows_affected": int(n_rows_affected),
                    "sample": sample,
                }
            )

    # 1. non_empty
    add_check(
        "non_empty",
        row_count > 0,
        "ERROR",
        "Dataset must contain at least one row.",
        {"row_count": row_count},
        n_rows_affected=row_count if row_count == 0 else 0,
        sample=_sample_rows(df),
    )

    # 2. unique_key
    key_cols = ["timestamp_utc", "location_id"]
    if row_count == 0:
        add_check(
            "unique_key",
            False,
            "ERROR",
            "Duplicate check skipped because dataset is empty.",
            {"duplicate_count": 0},
            row_selector="all",
            n_rows_affected=0,
            sample="[]",
        )
    elif not all(col in df.columns for col in key_cols):
        missing_cols = [col for col in key_cols if col not in df.columns]
        add_check(
            "unique_key",
            False,
            "ERROR",
            f"Required columns missing for unique key check: {', '.join(missing_cols)}.",
            {"missing_columns": missing_cols},
            row_selector="all",
            n_rows_affected=0,
            sample="[]",
        )
    else:
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        dup_rows = df[dup_mask]
        add_check(
            "unique_key",
            dup_rows.empty,
            "ERROR",
            "Duplicate (timestamp_utc, location_id) keys found.",
            {"duplicate_count": int(len(dup_rows))},
            row_selector="duplicates",
            n_rows_affected=int(len(dup_rows)),
            sample=_sample_rows(dup_rows),
        )

    # 3. timestamp_within_horizon
    try:
        timestamps = pd.to_datetime(df["timestamp_utc"], utc=True)
        min_ts = timestamps.min()
        max_exclusive = min_ts + timedelta(hours=settings.horizon_hours)
        within = (timestamps >= min_ts) & (timestamps < max_exclusive)
        bad = df[~within]
        add_check(
            "timestamp_within_horizon",
            bad.empty,
            "ERROR",
            "Timestamps must fall within [min_ts, min_ts + horizon_hours).",
            {
                "min_ts": min_ts.isoformat() if pd.notna(min_ts) else None,
                "max_exclusive": max_exclusive.isoformat() if pd.notna(min_ts) else None,
                "bad_count": int(len(bad)),
            },
            row_selector="outside_horizon",
            n_rows_affected=int(len(bad)),
            sample=_sample_rows(bad),
        )
    except Exception as exc:
        add_check(
            "timestamp_within_horizon",
            False,
            "ERROR",
            f"Failed to evaluate timestamps: {exc}",
            {"error": str(exc)},
            row_selector="all",
            n_rows_affected=0,
            sample="[]",
        )

    # 4. precip_prob_range
    precip_prob = pd.to_numeric(df.get("precip_prob"), errors="coerce")
    if precip_prob.isna().all():
        add_check(
            "precip_prob_range",
            False,
            "ERROR",
            "precip_prob values are missing or non-numeric.",
            {"scale": "unknown", "missing_count": int(precip_prob.isna().sum())},
            row_selector="missing",
            n_rows_affected=int(precip_prob.isna().sum()),
            sample=_sample_values(precip_prob.dropna().tolist()),
        )
    else:
        max_val = precip_prob.max()
        scale = "0-1" if max_val <= 1 else "0-100"
        if scale == "0-1":
            bad_mask = (precip_prob < 0) | (precip_prob > 1)
            lower, upper = 0.0, 1.0
        else:
            bad_mask = (precip_prob < 0) | (precip_prob > 100)
            lower, upper = 0.0, 100.0
        bad = df[bad_mask]
        add_check(
            "precip_prob_range",
            bad.empty,
            "ERROR",
            f"precip_prob must be within [{lower}, {upper}] based on detected scale {scale}.",
            {
                "scale": scale,
                "min": float(precip_prob.min()),
                "max": float(precip_prob.max()),
                "bad_count": int(len(bad)),
            },
            row_selector="out_of_range",
            n_rows_affected=int(len(bad)),
            sample=_sample_rows(bad),
        )

    # 5. precip_mm_nonnegative
    precip_mm = pd.to_numeric(df.get("precip_mm"), errors="coerce")
    neg_mask = precip_mm < 0
    neg_rows = df[neg_mask.fillna(False)]
    add_check(
        "precip_mm_nonnegative",
        neg_rows.empty,
        "ERROR",
        "precip_mm must be >= 0.",
        {"negative_count": int(len(neg_rows))},
        row_selector="negative_values",
        n_rows_affected=int(len(neg_rows)),
        sample=_sample_rows(neg_rows),
    )

    # 6. temp_reasonable
    temp_c = pd.to_numeric(df.get("temp_c"), errors="coerce")
    temp_mask = (temp_c < -60) | (temp_c > 60)
    temp_rows = df[temp_mask.fillna(False)]
    add_check(
        "temp_reasonable",
        temp_rows.empty,
        "WARN",
        "temp_c outside [-60, 60] indicates possible outliers.",
        {"outlier_count": int(len(temp_rows))},
        row_selector="outliers",
        n_rows_affected=int(len(temp_rows)),
        sample=_sample_rows(temp_rows),
    )

    # 7. missingness
    key_cols = ["timestamp_utc", "precip_prob", "precip_mm", "temp_c"]
    missing_rates: dict[str, float] = {}
    missing_violations: list[str] = []
    for col in key_cols:
        if col not in df.columns:
            rate = 1.0
        else:
            series = df[col]
            empty_mask = series.astype(str).str.strip().eq("")
            missing_mask = series.isna() | empty_mask
            rate = float(missing_mask.mean())
        missing_rates[col] = rate
        if rate > 0.01:
            missing_violations.append(col)
    passed_missing = len(missing_violations) == 0
    add_check(
        "missingness",
        passed_missing,
        "ERROR",
        "Missingness for key columns must be <= 1%.",
        {"missing_rates": missing_rates, "violations": missing_violations},
        row_selector="missing",
        n_rows_affected=int(df[key_cols].isna().sum().sum()) if not passed_missing else 0,
        sample=_sample_values(missing_violations),
    )

    # 8. reconciliation_count
    try:
        raw_count = _load_raw_row_count(run_date_str, settings)
        counts_match = raw_count == row_count
        add_check(
            "reconciliation_count",
            counts_match,
            "ERROR",
            "Raw hourly count must equal processed row count.",
            {"raw_count": raw_count, "processed_count": row_count},
            row_selector="all",
            n_rows_affected=abs(raw_count - row_count),
            sample=_sample_values([raw_count, row_count]),
        )
    except Exception as exc:
        add_check(
            "reconciliation_count",
            False,
            "ERROR",
            f"Failed reconciliation_count check: {exc}",
            {"error": str(exc)},
            row_selector="all",
            n_rows_affected=0,
            sample="[]",
        )

    checks_passed = sum(1 for check in checks if check["passed"])
    errors_failed = sum(1 for check in checks if (not check["passed"]) and check["severity"] == "ERROR")
    warns_failed = sum(1 for check in checks if (not check["passed"]) and check["severity"] == "WARN")
    status = "pass" if errors_failed == 0 else "fail"

    validation_payload = {
        "run_date": run_date_str,
        "generated_at_utc": now_utc().isoformat(),
        "status": status,
        "row_count": row_count,
        "errors_failed": errors_failed,
        "warns_failed": warns_failed,
        "checks": checks,
        "inputs": {"forecast_hourly": str(processed_path)},
        "outputs": {"validation": str(validation_path), "exceptions": str(exceptions_path)},
    }

    write_json(validation_path, validation_payload)
    exceptions_df = pd.DataFrame(exceptions, columns=EXCEPTION_COLUMNS)
    write_csv(exceptions_path, exceptions_df)

    logger.info("validate_end", extra={"status": status, "row_count": row_count})

    return {
        "status": status,
        "checks_passed": checks_passed,
        "errors_failed": errors_failed,
        "warns_failed": warns_failed,
        "output_paths": {
            "validation": validation_path,
            "exceptions": exceptions_path,
        },
        "row_count": row_count,
    }
