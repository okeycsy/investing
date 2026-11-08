"""Markdown rendering helpers."""
from __future__ import annotations

import math
from typing import Iterable, Optional

import pandas as pd


def money(x, unit="M", digits=0) -> str:
    """Format raw USD into $xxM / $x.xB."""
    try:
        v = float(x)
    except Exception:  # noqa: BLE001
        return "–"
    if math.isnan(v):
        return "–"
    if abs(v) >= 1e9:
        return f"${v/1e9:,.{max(digits,1)}f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:,.{digits}f}M"
    return f"${v:,.0f}"


def pct(x, digits=1, sign=False) -> str:
    try:
        v = float(x)
    except Exception:  # noqa: BLE001
        return "–"
    if math.isnan(v):
        return "–"
    return f"{v:+.{digits}f}%" if sign else f"{v:.{digits}f}%"


def num(x, digits=2) -> str:
    try:
        v = float(x)
    except Exception:  # noqa: BLE001
        return "–"
    if math.isnan(v):
        return "–"
    return f"{v:,.{digits}f}"


def table(headers: list[str], rows: Iterable[Iterable]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def df_table(df: pd.DataFrame, fmt: Optional[dict] = None, index_name: str = "") -> str:
    """DataFrame → markdown table. fmt maps column → callable."""
    fmt = fmt or {}
    headers = [index_name] + [str(c) for c in df.columns]
    rows = []
    for idx, r in df.iterrows():
        cells = [str(idx.date()) if hasattr(idx, "date") else str(idx)]
        for c in df.columns:
            f = fmt.get(c)
            cells.append(f(r[c]) if f else (num(r[c]) if isinstance(r[c], (int, float)) else str(r[c])))
        rows.append(cells)
    return table(headers, rows)
