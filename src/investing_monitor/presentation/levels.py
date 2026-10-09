from __future__ import annotations

from investing_monitor.domain.levels import ClusteredLevel, PriceLevels
from investing_monitor.domain.models import Position
from investing_monitor.presentation.market_context import pct_label, price_label


def levels_text(levels: PriceLevels, current_price: float | None) -> str:
    """Key technical levels around the current price, one compact block."""
    reference = current_price if current_price is not None else levels.last_close
    lines = []
    structural = _structural_line(levels, reference)
    if structural:
        lines.append(structural)
    averages = _moving_average_line(levels, reference)
    if averages:
        lines.append(averages)
    yearly = _yearly_range_line(levels, reference)
    if yearly:
        lines.append(yearly)
    if not lines:
        return ""
    return "🧱 *주요 레벨*\n" + "\n".join(lines)


def position_text(position: Position, current_price: float | None) -> str:
    if current_price is None or position.average_price <= 0:
        return ""
    change = (current_price / position.average_price - 1) * 100
    line = (
        f"💼 *평단 {price_label(position.average_price)} 대비 {pct_label(change)}*"
    )
    if position.shares:
        profit = (current_price - position.average_price) * position.shares
        line += f" · {position.shares:,}주 · 평가손익 {_signed_money(profit)}"
    return line


def _structural_line(levels: PriceLevels, reference: float | None) -> str:
    parts = []
    if levels.supports:
        parts.append(
            "지지 " + " · ".join(_level_label(level, reference) for level in levels.supports)
        )
    if levels.resistances:
        parts.append(
            "저항 " + " · ".join(_level_label(level, reference) for level in levels.resistances)
        )
    if levels.atr14:
        parts.append(f"ATR(14) {price_label(levels.atr14)}")
    return " | ".join(parts)


def _moving_average_line(levels: PriceLevels, reference: float | None) -> str:
    parts = []
    for label, value in (
        ("SMA20", levels.sma20),
        ("SMA50", levels.sma50),
        ("SMA200", levels.sma200),
    ):
        if value is None:
            continue
        marker = ""
        if reference is not None:
            marker = "↑" if reference >= value else "↓"
        parts.append(f"{label} {price_label(value)}{marker}")
    if not parts:
        return ""
    return " · ".join(parts) + " (↑=현재가가 위)"


def _yearly_range_line(levels: PriceLevels, reference: float | None) -> str:
    if levels.high_52w is None or levels.low_52w is None:
        return ""
    line = f"52주 {price_label(levels.low_52w)} ~ {price_label(levels.high_52w)}"
    if reference is not None and levels.high_52w > levels.low_52w:
        line += (
            f" · 고점 대비 {pct_label((reference / levels.high_52w - 1) * 100)}"
            f" · 저점 대비 {pct_label((reference / levels.low_52w - 1) * 100)}"
        )
    return line


def _level_label(level: ClusteredLevel, reference: float | None) -> str:
    label = f"{price_label(level.price)}({level.touches}회)"
    return label


def _signed_money(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.0f}"
