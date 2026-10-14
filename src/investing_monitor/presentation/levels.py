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


def nearest_levels_line(levels: PriceLevels, price: float | None) -> str:
    """The two levels that matter from the current price, one line.

    e.g. '🧱 아래 지지 $234.01 (-1.2%) · 위 저항 $248.79 (+10.4%)'
    All tracked levels (supports, resistances, 52-week extremes) are
    partitioned against the live price, so a support the price has already
    fallen through is never shown as support again.
    """
    reference = price if price is not None else levels.last_close
    if reference is None:
        return ""
    candidates: list[tuple[str, float]] = []
    for level in levels.supports:
        # A support the price has fallen through is overhead supply now.
        name = "지지" if level.price < reference else "이탈 지지"
        candidates.append((f"{name} {price_label(level.price)}({level.touches}회)", level.price))
    for level in levels.resistances:
        name = "저항" if level.price > reference else "돌파 저항"
        candidates.append((f"{name} {price_label(level.price)}({level.touches}회)", level.price))
    if levels.low_52w is not None:
        candidates.append((f"52주 저점 {price_label(levels.low_52w)}", levels.low_52w))
    if levels.high_52w is not None:
        candidates.append((f"52주 고점 {price_label(levels.high_52w)}", levels.high_52w))
    below = [item for item in candidates if item[1] < reference]
    above = [item for item in candidates if item[1] > reference]
    lines = ["🧱 *레벨*"]
    if below:
        name, value = max(below, key=lambda item: item[1])
        lines.append(f"▼ {name} ({pct_label((value / reference - 1) * 100)})")
    else:
        lines.append("▼ 아래 추적 레벨 없음")
    if above:
        name, value = min(above, key=lambda item: item[1])
        lines.append(f"▲ {name} ({pct_label((value / reference - 1) * 100)})")
    return "\n".join(lines)


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
    # Whether a level counts as broken at the close is a verdict, not a touch
    # test: use a tight margin so closing $1+ through a level is never called
    # defended (2026-10-12 closed 239.36 under the 240.72 support yet the wide
    # touch band labeled it "사수").
    verdict_margin = max((levels.atr14 or 0.0) * 0.02, close_price * 0.001)
    day_range = day_high - day_low
    close_position = (
        (close_price - day_low) / day_range if day_range > 0 else 0.5
    )
    lines: list[str] = []
    for support in levels.supports:
        if day_low > support.price + tolerance:
            continue
        if close_price < support.price - verdict_margin:
            lines.append(
                f"⚠️ 지지 {price_label(support.price)}({support.touches}회) 이탈 마감"
                f" — 종가 {price_label(close_price)} · 저가 {price_label(day_low)}"
            )
        elif close_price <= support.price + verdict_margin:
            lines.append(
                f"🟡 지지 {price_label(support.price)}({support.touches}회)에 걸친 경계 마감"
                f" — 종가 {price_label(close_price)}"
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
        if close_price > resistance.price + verdict_margin:
            lines.append(
                f"🚀 저항 {price_label(resistance.price)}({resistance.touches}회) 돌파 마감"
                f" — 종가 {price_label(close_price)}"
            )
        elif close_price >= resistance.price - verdict_margin:
            lines.append(
                f"🟡 저항 {price_label(resistance.price)}({resistance.touches}회)에 걸친 경계 마감"
                f" — 종가 {price_label(close_price)}"
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
