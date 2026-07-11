from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


MAX_COMPLETED_KEYS = 240


def scheduled_dispatch_key() -> str:
    if os.environ.get("SCHEDULED_RUN", "").lower() != "true":
        return ""
    return os.environ.get("SCHEDULE_DISPATCH_KEY", "").strip()


def _load(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def should_skip(path: Path, logger=None) -> bool:
    key = scheduled_dispatch_key()
    if not key:
        return False

    state = _load(path)
    completed = state.get("completed_dispatches", {})
    if key in completed:
        if logger:
            logger.info(f"Scheduled dispatch already completed: {key}")
        return True
    return False


def mark_completed(path: Path, logger=None) -> None:
    key = scheduled_dispatch_key()
    if not key:
        return

    state = _load(path)
    completed = state.setdefault("completed_dispatches", {})
    completed[key] = datetime.now(timezone.utc).isoformat()

    if len(completed) > MAX_COMPLETED_KEYS:
        keep = sorted(completed.items(), key=lambda item: item[1])[-MAX_COMPLETED_KEYS:]
        state["completed_dispatches"] = dict(keep)

    _save(path, state)
    if logger:
        logger.info(f"Scheduled dispatch marked complete: {key}")
