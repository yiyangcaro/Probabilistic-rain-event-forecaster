from __future__ import annotations

import argparse
from datetime import date

from .config import Settings
from .pipeline import run_pipeline

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="rain_alert")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run end to end pipeline")
    run.add_argument("--run-date", type=str, default=str(date.today()))
    run.add_argument("--config", type=str, default="")

    sub.add_parser("help", help="Show help")

    return p.parse_args()

def main() -> int:
    args = parse_args()
    _ = Settings.load(getattr(args, "config", ""))
    settings = Settings.load(getattr(args, "config", ""))

    if args.cmd == "run":
        try:
            result = run_pipeline(args.run_date, settings)
        except Exception:
            return 1
        return 0 if result.get("status") == "pass" else 2

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
