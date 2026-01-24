from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

import pandas as pd

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a simple run report.")
    parser.add_argument("run_date", type=str, help="Run date in YYYY-MM-DD format")
    return parser.parse_args()

def main() -> int:
    args = parse_args()
    run_date = args.run_date

    hourly_path = Path("data/star") / run_date / "fact_forecast_hourly.csv"
    validation_path = Path("reports/validation") / f"validation_{run_date}.json"
    exceptions_path = Path("reports/exceptions") / f"exceptions_{run_date}.csv"

    missing = []
    if not hourly_path.exists():
        missing.append(str(hourly_path))
    if not validation_path.exists():
        missing.append(str(validation_path))
    if missing:
        print("Missing required input(s):")
        for path in missing:
            print(f"- {path}")
        return 1

    hourly = pd.read_csv(hourly_path)
    validation = json.loads(validation_path.read_text(encoding="utf-8"))

    max_precip_prob = float(hourly["precip_prob"].max()) if "precip_prob" in hourly.columns else float("nan")
    total_precip_mm = float(hourly["precip_mm"].sum()) if "precip_mm" in hourly.columns else float("nan")

    first_high_risk = "N A"
    if "risk_level" in hourly.columns:
        high_mask = hourly["risk_level"].astype(str).str.strip().eq("High")
        if high_mask.any():
            if "timestamp_utc" in hourly.columns:
                first_high_risk = str(hourly.loc[high_mask, "timestamp_utc"].iloc[0])
            else:
                first_high_risk = "High"

    validation_status = validation.get("status", "unknown")

    report_dir = Path("reports/run_reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    md_path = report_dir / f"run_report_{run_date}.md"
    csv_path = report_dir / f"run_report_{run_date}.csv"

    exceptions_display = str(exceptions_path) if exceptions_path.exists() else "not_found"

    md_content = "\n".join(
        [
            f"# Run Report {run_date}",
            "",
            f"- run_date: {run_date}",
            f"- validation_status: {validation_status}",
            f"- max_precip_prob: {max_precip_prob}",
            f"- total_precip_mm: {total_precip_mm}",
            f"- first_high_risk_timestamp: {first_high_risk}",
            "",
            "Artifacts:",
            f"- hourly_fact: {hourly_path}",
            f"- validation: {validation_path}",
            f"- exceptions: {exceptions_display}",
            "",
        ]
    )
    md_path.write_text(md_content, encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_date",
                "validation_status",
                "max_precip_prob",
                "total_precip_mm",
                "first_high_risk_timestamp",
                "hourly_fact_path",
                "validation_path",
                "exceptions_path",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_date": run_date,
                "validation_status": validation_status,
                "max_precip_prob": max_precip_prob,
                "total_precip_mm": total_precip_mm,
                "first_high_risk_timestamp": first_high_risk,
                "hourly_fact_path": str(hourly_path),
                "validation_path": str(validation_path),
                "exceptions_path": exceptions_display,
            }
        )

    print(f"Wrote {md_path}")
    print(f"Wrote {csv_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
