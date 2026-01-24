from __future__ import annotations

import json
from pathlib import Path
from typing import Any

def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target

def write_json(path: str | Path, payload: Any) -> Path:
    target = Path(path)
    ensure_dir(target.parent)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return target

def write_csv(path: str | Path, frame: Any) -> Path:
    target = Path(path)
    ensure_dir(target.parent)
    frame.to_csv(target, index=False)
    return target
