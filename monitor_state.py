from __future__ import annotations

import json
from pathlib import Path


def default_state() -> dict:
    return {
        "last_news_hashes": [],
        "last_insider_hashes": [],
        "last_13f_hashes": [],
        "price_history": [],
        "price_alert_max_pct": 0,
        "price_alert_direction": "",
        "price_alert_date": "",
        "pending_morning_alert": None,
        "dca_shares": 0.0,
        "dca_avg_price": 0.0,
        "dca_history": [],
    }


def default_weekly_state() -> dict:
    return {
        "week_start": "",
        "alerts_fired": [],
        "insider_trades": [],
        "news_headlines": [],
        "rsi_readings": [],
        "pcr_readings": [],
        "short_readings": [],
    }


def load_state_file(path: Path, default_factory) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return default_factory()


def save_state_file(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False))
