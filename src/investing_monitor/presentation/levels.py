from __future__ import annotations

from investing_monitor.domain.levels import ClusteredLevel, PriceLevels
from investing_monitor.domain.models import Position
from investing_monitor.presentation.market_context import pct_label, price_label


def levels_text(levels: PriceLevels, current_price: float | None) -> str:
    """Key technical levels around the current price, one readable block."""
    reference = current_price if current_price is not None else levels.last_close
    lines = []
    if levels.supports:
        lines.append(
            "지지  " + "  |  ".join(
                _level_label(level, reference) for level in levels.supports
            )
        )
    if levels.resistances:
        lines.append(
            "저항  " + "  |  ".join(
                _level_label(level, reference) for level in levels.resistances
            )
        )
    averages = _moving_average_line(levels, reference)
    if averages:
        lines.append("이평  " + averages)
    yearly = _yearly_range_line(levels, reference)
    if yearly:
        lines.append("52주  " + yearly)
    if levels.atr14:
        lines.append(f"ATR(14) {price_label(levels.atr14)} · 통상 하루 진폭")
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


def day_level_review(
    levels: PriceLevels,
    close_price: float | None,
    day_low: float | None,
    day_high: float | None,
) -> str:
    """How today's session interacted with the pre-computed level map."""
    if close_price is None or day_low is None or day_high is None:
        return ""
    tolerance = max((levels.atr14 or 0.0) * 0.15, close_price * 0.003)
    day_range = day_high - day_low
    close_position = (
        (close_price - day_low) / day_range if day_range > 0 else 0.5
    )
    lines: list[str] = []
    for support in levels.supports:
        if day_low > support.price + tolerance:
            continue
        if close_price < support.price - tolerance:
            lines.append(
                f"⚠️ 지지 {price_label(support.price)}({support.touches}회) 이탈 마감"
                f" — 저가 {price_label(day_low)}"
            )
        else:
            qualifier = ""
            if close_position <= 0.35:
                qualifier = " · 다만 반등 탄력 없이 저가권 마감"
            elif close_position >= 0.6:
                qualifier = " · 저가에서 뚜렷하게 반등"
            lines.append(
                f"🛡️ 지지 {price_label(support.price)}({support.touches}회) 테스트 후 사수"
                f" — 저가 {price_label(day_low)}{qualifier}"
            )
    for resistance in levels.resistances:
        if day_high < resistance.price - tolerance:
            continue
        if close_price > resistance.price + tolerance:
            lines.append(
                f"🚀 저항 {price_label(resistance.price)}({resistance.touches}회) 돌파 마감"
            )
        else:
            lines.append(
                f"🧲 저항 {price_label(resistance.price)}({resistance.touches}회) 터치 후 반락"
                f" — 고가 {price_label(day_high)}"
            )
    previous_close = levels.last_close
    if previous_close is not None:
        for label, value in (
            ("SMA20", levels.sma20),
            ("SMA50", levels.sma50),
            ("SMA200", levels.sma200),
        ):
            if value is None:
                continue
            if previous_close < value <= close_price:
                lines.append(f"📈 {label} {price_label(value)} 상향 돌파 마감")
            elif previous_close > value >= close_price:
                lines.append(f"📉 {label} {price_label(value)} 하향 이탈 마감")
    if not lines:
        return ""
    return "📐 *오늘의 레벨 리뷰*\n" + "\n".join(lines)


def _moving_average_line(levels: PriceLevels, reference: float | None) -> str:
    parts = []
    for label, value in (
        ("20일", levels.sma20),
        ("50일", levels.sma50),
        ("200일", levels.sma200),
    ):
        if value is None:
            continue
        marker = ""
        if reference is not None:
            marker = "↑" if reference >= value else "↓"
        parts.append(f"{label} {price_label(value)}{marker}")
    if not parts:
        return ""
    return " · ".join(parts) + "  (↓=현재가가 아래)"


def _yearly_range_line(levels: PriceLevels, reference: float | None) -> str:
    if levels.high_52w is None or levels.low_52w is None:
        return ""
    line = f"{price_label(levels.low_52w)} ~ {price_label(levels.high_52w)}"
    if reference is not None and levels.high_52w > levels.low_52w:
        line += (
            f"  (고점 {pct_label((reference / levels.high_52w - 1) * 100)}"
            f" / 저점 {pct_label((reference / levels.low_52w - 1) * 100)})"
        )
    return line


def _level_label(level: ClusteredLevel, reference: float | None) -> str:
    label = f"{price_label(level.price)} ({level.touches}회"
    if reference:
        label += f" · {pct_label((level.price / reference - 1) * 100)}"
    return label + ")"


def _signed_money(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.0f}"
