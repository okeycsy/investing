from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime

from investing_monitor.domain.levels import PriceLevels
from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    LevelEventSignal,
    MarketSnapshot,
    Position,
    PriceBandSignal,
    RapidMoveSignal,
    VolumeSignal,
    VolumeSnapshot,
)
from investing_monitor.presentation.levels import (
    nearest_levels_line,
    position_text,
)
from investing_monitor.domain.policies import (
    RelativeAssessment,
    SituationAssessment,
    VolumeAssessment,
)
from investing_monitor.domain.situation import MarketContextDelta
from investing_monitor.presentation.market_context import (
    pct_label,
    price_banner,
    price_label,
    relative_block,
    shares_label,
)
from investing_monitor.presentation.timing import (
    DELAYED_DETECTION_SECONDS,
    delay_seconds,
    duration_label,
    observation_context,
    session_label,
    timestamp,
)


def build_price_band_message(
    signal: PriceBandSignal,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
    *,
    detection_delay_seconds: int = 0,
    detected_at: datetime | None = None,
    latest_snapshot: MarketSnapshot | None = None,
    latest_relative: RelativeAssessment | None = None,
    latest_situation: SituationAssessment | None = None,
    situation: SituationAssessment | None = None,
    delta: MarketContextDelta | None = None,
    event_snapshot: MarketSnapshot | None = None,
    event_price: float | None = None,
    latest_price: float | None = None,
    reference_close: float | None = None,
    levels: PriceLevels | None = None,
    position: Position | None = None,
) -> dict:
    """Compact band alert: what happened, vs market, volume, nearest levels.

    The grade/sensitivity/delta prose of the first design did not help a
    decision, so each line now answers one question and everything else is
    gone (2026-10-15 redesign).
    """
    direction_icon = "📈" if signal.direction is Direction.UP else "📉"
    direction_label = "상승" if signal.direction is Direction.UP else "하락"
    signed_level = signal.level if signal.direction is Direction.UP else -signal.level
    reversal = " · 장중 방향 반전" if signal.is_reversal else ""
    if detected_at is not None:
        detection_delay_seconds = delay_seconds(signal.observed_at, detected_at)
    if latest_snapshot is not None and (
        latest_snapshot.ticker != signal.ticker
        or latest_snapshot.trading_date != signal.trading_date
        or latest_snapshot.observed_at < signal.observed_at
    ):
        raise ValueError("latest snapshot must belong to the same event timeline")
    delayed = detection_delay_seconds > DELAYED_DETECTION_SECONDS
    historical = delayed or (
        latest_snapshot is not None and latest_snapshot.observed_at > signal.observed_at
    )
    event_label = "도달 기록" if historical else "구간 진입"
    title = f"${signal.ticker} {signed_level:+.1f}% {direction_label} {event_label}"
    if delayed:
        title += " · 지연 확인"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{direction_icon} {title}{reversal}",
            },
        },
        _context(
            _compact_timing(
                signal.observed_at,
                session_label(signal.session),
                detected_at,
                detection_delay_seconds,
            )
        ),
    ]

    latest_status = ""
    price_lines = []
    if event_price is not None:
        price_lines.append(
            price_banner(
                event_price,
                event_snapshot.change_pct if event_snapshot is not None else None,
                reference_close,
            )
        )
    if historical and latest_snapshot is not None:
        latest_status = _band_status(signal, latest_snapshot)
        latest_line = f"마지막 관측 · {latest_status}"
        if latest_price is not None:
            latest_line = (
                f"마지막 관측 {price_label(latest_price)}"
                f" ({pct_label(latest_snapshot.change_pct)}) · {latest_status}"
            )
        price_lines.append(latest_line)
    if price_lines:
        blocks.append(_section("\n".join(price_lines)))

    display_snapshot = (
        latest_snapshot if historical and latest_snapshot is not None else event_snapshot
    )
    display_relative = (
        latest_relative if historical and latest_relative is not None else relative
    )
    blocks.append(_section(relative_block(display_snapshot, display_relative)))

    if volume is not None and volume_assessment.is_ready:
        ratio = volume_assessment.ratio or 0.0
        prefix = "🔥" if volume_assessment.is_exploded else "🔊"
        blocks.append(
            _section(
                f"{prefix} *거래량*  동시간대 평시 *{ratio:.1f}배*"
                f"  ·  누적 {shares_label(volume.observed_volume)} 주"
            )
        )

    display_price = latest_price if latest_price is not None else event_price
    if levels is not None:
        nearest = nearest_levels_line(levels, display_price)
        if nearest:
            blocks.append(_section(nearest))
    if position is not None:
        rendered_position = position_text(position, display_price)
        if rendered_position:
            blocks.append(_section(rendered_position))

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *최근 확인된 관련 사건*"))
        for catalyst in selected:
            blocks.append(_section(_catalyst_text(catalyst)))

    fallback = f"{title} | 관측 {timestamp(signal.observed_at)}"
    if event_price is not None and event_snapshot is not None:
        fallback += f" | {price_label(event_price)} ({pct_label(event_snapshot.change_pct)})"
    if detected_at is not None:
        fallback += f" | 확인 {timestamp(detected_at)}"
    if historical and latest_snapshot is not None:
        fallback += f" | 마지막 관측 {timestamp(latest_snapshot.observed_at)}: {latest_status}"
    return {
        "text": fallback,
        "blocks": blocks,
    }


def build_volume_message(
    signal: VolumeSignal,
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    volume: VolumeSnapshot,
    volume_assessment: VolumeAssessment,
    *,
    detection_delay_seconds: int = 0,
    detected_at: datetime | None = None,
    situation: SituationAssessment | None = None,
    price: float | None = None,
    reference_close: float | None = None,
    levels: PriceLevels | None = None,
    position: Position | None = None,
) -> dict:
    observed_at = volume.observed_at or signal.observed_at
    if detected_at is not None:
        detection_delay_seconds = delay_seconds(observed_at, detected_at)
    ratio = volume_assessment.ratio or 0.0
    delayed = detection_delay_seconds > DELAYED_DETECTION_SECONDS
    title = f"${signal.ticker} 거래량 {ratio:.1f}배 확대"
    if delayed:
        title += " · 지연 확인"
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🔥 {title}"},
        },
        _context(
            _compact_timing(
                observed_at,
                "거래량 기준",
                detected_at,
                detection_delay_seconds,
            )
        ),
    ]
    if price is not None:
        blocks.append(
            _section(price_banner(price, snapshot.change_pct, reference_close))
        )
    blocks.append(
        _section(
            f"🔥 *거래량*  동시간대 평시 *{ratio:.1f}배*\n"
            f"누적 {shares_label(volume.observed_volume)} 주  ·  "
            f"{volume.baseline_sessions}거래일 평균 {shares_label(volume.expected_volume)} 주"
        )
    )
    blocks.append(_section(relative_block(snapshot, relative)))
    if levels is not None:
        nearest = nearest_levels_line(levels, price)
        if nearest:
            blocks.append(_section(nearest))
    if position is not None:
        rendered_position = position_text(position, price)
        if rendered_position:
            blocks.append(_section(rendered_position))
    fallback = f"{title} | 자료 {timestamp(observed_at)}"
    if detected_at is not None:
        fallback += f" | 확인 {timestamp(detected_at)}"
    return {
        "text": fallback,
        "blocks": blocks,
    }


def build_green_cross_message(
    snapshot: MarketSnapshot,
    *,
    price: float | None = None,
    reference_close: float | None = None,
    detected_at: datetime | None = None,
) -> dict:
    title = f"${snapshot.ticker} 양전 전환 {pct_label(snapshot.change_pct)}"
    detection_delay = (
        delay_seconds(snapshot.observed_at, detected_at) if detected_at else 0
    )
    price_line = price_banner(price, snapshot.change_pct, reference_close)
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"💚 {title}"}},
        _context(
            observation_context(
                snapshot.observed_at,
                session_label(snapshot.session),
                detected_at,
                detection_delay,
            )
        ),
    ]
    if price_line:
        blocks.append(_section(price_line))
    blocks.append(
        _context("당일 첫 양전 확인(5분봉 종가 기준) · 이후 흐름은 마감 브리프에서 정리")
    )
    fallback = f"{title}"
    if price is not None:
        fallback += f" | {price_label(price)}"
    fallback += f" | 관측 {timestamp(snapshot.observed_at)}"
    return {"text": fallback, "blocks": blocks}


_LEVEL_LABELS = {
    "support": "지지",
    "resistance": "저항",
    "sma20": "SMA20",
    "sma50": "SMA50",
    "sma200": "SMA200",
    "52w-low": "52주 저점",
    "52w-high": "52주 고점",
}


def build_level_event_message(
    signal: LevelEventSignal,
    snapshot: MarketSnapshot,
    catalysts: Sequence[Catalyst],
    *,
    detected_at: datetime | None = None,
    reference_close: float | None = None,
    levels: PriceLevels | None = None,
    position: Position | None = None,
) -> dict:
    label = _LEVEL_LABELS.get(signal.kind, signal.kind)
    if signal.kind == "52w-low":
        headline = "52주 신저가"
        icon = "🚨"
    elif signal.kind == "52w-high":
        headline = "52주 신고가"
        icon = "🎉"
    else:
        action = {
            ("support", Direction.DOWN): "하향 이탈",
            ("support", Direction.UP): "회복",
            ("resistance", Direction.UP): "상향 돌파",
        }.get(
            (signal.kind, signal.direction),
            "상향 돌파" if signal.direction is Direction.UP else "하향 이탈",
        )
        headline = f"{label} {price_label(signal.level_price)} {action}"
        icon = "🧱" if signal.kind in {"support", "resistance"} else (
            "📈" if signal.direction is Direction.UP else "📉"
        )
    title = f"${signal.ticker} {headline}"
    detection_delay = (
        delay_seconds(signal.observed_at, detected_at) if detected_at else 0
    )
    touches = f" · 최근 저가/고가 {signal.touches}회 테스트 레벨" if signal.touches else ""
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"{icon} {title}"}},
        _context(
            observation_context(
                signal.observed_at,
                session_label(signal.session),
                detected_at,
                detection_delay,
            )
        ),
        _section(
            price_banner(signal.close_price, snapshot.change_pct, reference_close)
            + f"\n기준 레벨 {price_label(signal.level_price)}{touches}"
        ),
    ]
    next_line = _next_level_line(signal, levels)
    if next_line:
        blocks.append(_section(next_line))
    if position is not None:
        rendered_position = position_text(position, signal.close_price)
        if rendered_position:
            blocks.append(_section(rendered_position))
    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *최근 확인된 관련 사건*"))
        blocks.extend(_section(_catalyst_text(catalyst)) for catalyst in selected)
    blocks.append(_context("5분봉 종가 기준 자동 판정 · 되돌림 가능"))
    fallback = f"{title} | {price_label(signal.close_price)} ({pct_label(snapshot.change_pct)})"
    fallback += f" | 관측 {timestamp(signal.observed_at)}"
    if detected_at is not None:
        fallback += f" | 확인 {timestamp(detected_at)}"
    return {"text": fallback, "blocks": blocks}


def _next_level_line(
    signal: LevelEventSignal,
    levels: PriceLevels | None,
) -> str:
    if levels is None:
        return ""
    close = signal.close_price
    if signal.direction is Direction.DOWN:
        candidates = [
            (f"지지 {price_label(item.price)}({item.touches}회)", item.price)
            for item in levels.supports
            if item.price < close
        ]
        if levels.low_52w is not None and levels.low_52w < close:
            candidates.append((f"52주 저점 {price_label(levels.low_52w)}", levels.low_52w))
        if not candidates:
            return "⬇️ 아래쪽에 추적 중인 다음 레벨 없음"
        name, price = max(candidates, key=lambda item: item[1])
        return f"⬇️ 다음 레벨: {name} · 현재가에서 {pct_label((price / close - 1) * 100)}"
    candidates = [
        (f"저항 {price_label(item.price)}({item.touches}회)", item.price)
        for item in levels.resistances
        if item.price > close
    ]
    if levels.high_52w is not None and levels.high_52w > close:
        candidates.append((f"52주 고점 {price_label(levels.high_52w)}", levels.high_52w))
    if not candidates:
        return "⬆️ 위쪽에 추적 중인 다음 레벨 없음"
    name, price = min(candidates, key=lambda item: item[1])
    return f"⬆️ 다음 레벨: {name} · 현재가에서 {pct_label((price / close - 1) * 100)}"


def build_rapid_move_message(
    signal: RapidMoveSignal,
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    catalysts: Sequence[Catalyst],
    *,
    detected_at: datetime | None = None,
    situation: SituationAssessment | None = None,
    price: float | None = None,
    reference_close: float | None = None,
    levels: PriceLevels | None = None,
    position: Position | None = None,
) -> dict:
    direction_icon = "📈" if signal.direction is Direction.UP else "📉"
    direction_label = "급등" if signal.direction is Direction.UP else "급락"
    title = (
        f"${signal.ticker} 15분 {direction_label} 감지 "
        f"{signal.change_15m_pct:+.1f}%"
    )
    detection_delay = (
        delay_seconds(signal.observed_at, detected_at) if detected_at else 0
    )
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"⚡ {title}"},
        },
        _context(
            observation_context(
                signal.observed_at,
                session_label(signal.session),
                detected_at,
                detection_delay,
            )
        ),
        _section(
            f"{direction_icon} *15분간 {pct_label(signal.change_15m_pct)}*"
            f"  ·  같은 15분 거래량 평시 봉의 *{signal.volume_15m_ratio:.1f}배*"
        ),
        _section(price_banner(price, snapshot.change_pct, reference_close))
        if price is not None
        else _section(relative_block(snapshot, relative)),
    ]
    if price is not None:
        blocks.append(_section(relative_block(snapshot, relative)))
    if levels is not None:
        nearest = nearest_levels_line(levels, price)
        if nearest:
            blocks.append(_section(nearest))
    if position is not None:
        rendered_position = position_text(position, price)
        if rendered_position:
            blocks.append(_section(rendered_position))
    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *최근 확인된 관련 사건*"))
        blocks.extend(_section(_catalyst_text(catalyst)) for catalyst in selected)
    blocks.append(_context("15분 속도·거래량 자동 감지 · 원인 미확정"))
    fallback = (
        f"{title} | 거래량 {signal.volume_15m_ratio:.1f}배 "
        f"| 관측 {timestamp(signal.observed_at)}"
    )
    if detected_at is not None:
        fallback += f" | 확인 {timestamp(detected_at)}"
    return {"text": fallback, "blocks": blocks}


def _compact_timing(
    observed_at: datetime,
    label: str,
    detected_at: datetime | None,
    detection_delay_seconds: int,
) -> str:
    line = f"{label} · {timestamp(observed_at)}"
    if detected_at is not None and detection_delay_seconds > DELAYED_DETECTION_SECONDS:
        line += (
            f" · 확인 {timestamp(detected_at)}"
            f" (지연 {duration_label(detection_delay_seconds)})"
        )
    return line


def _catalyst_text(catalyst: Catalyst) -> str:
    impact_icon = {
        "strengthen": "🟢",
        "neutral": "⚪",
        "risk": "🟠",
        "damage": "🔴",
    }[catalyst.impact.value]
    return (
        f"{impact_icon} *<{catalyst.source_url}|{catalyst.headline}>*\n"
        f"{catalyst.summary}\n"
        f"_{catalyst.source_name} · 발표 {timestamp(catalyst.published_at)}_"
    )


def _band_status(signal: PriceBandSignal, snapshot: MarketSnapshot) -> str:
    change = round(snapshot.change_pct, 10)
    if signal.direction is Direction.UP:
        return (
            f"+{signal.level:.1f}% 구간 유지"
            if change >= signal.level else f"+{signal.level:.1f}% 구간 아래로 되돌림"
        )
    return (
        f"-{signal.level:.1f}% 구간 유지"
        if change <= -signal.level else f"-{signal.level:.1f}% 구간에서 반등"
    )


def _direction_text(snapshot: MarketSnapshot) -> str:
    return {
        Direction.UP: "📈 양전",
        Direction.DOWN: "📉 음전",
        Direction.FLAT: "➖ 보합",
    }[snapshot.direction]


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}
