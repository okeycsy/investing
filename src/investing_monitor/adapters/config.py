from __future__ import annotations

import os
from pathlib import Path

from investing_monitor.domain.models import InstrumentProfile


def load_instrument_profile(path: str | Path = "monitor_config.md") -> InstrumentProfile:
    values = _read_key_values(Path(path))
    ticker = _ticker(os.environ.get("MONITOR_TICKER") or values.get("ticker") or "VRT")
    benchmark = _ticker(
        os.environ.get("MONITOR_BENCHMARK") or values.get("benchmark") or "SOXX"
    )
    peer_source = (
        os.environ.get("MONITOR_PEER_TICKERS")
        or values.get("peer_tickers")
        or "ETN,NVT,GEV"
    )
    peers = tuple(
        dict.fromkeys(
            _ticker(item)
            for item in peer_source.replace("/", ",").split(",")
            if _ticker(item)
        )
    )
    return InstrumentProfile(ticker=ticker, benchmark=benchmark, peers=peers)


def _read_key_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", "```")):
            continue
        line = line.lstrip("-* ").strip()
        delimiter = ":" if ":" in line else "=" if "=" in line else ""
        if not delimiter:
            continue
        key, value = line.split(delimiter, 1)
        values[key.strip().lower().replace("-", "_")] = _clean(value)
    return values


def _clean(value: str) -> str:
    return value.strip().strip("`").strip().strip("\"'").split(" #", 1)[0].strip()


def _ticker(value: str) -> str:
    return value.strip().upper().lstrip("$")
