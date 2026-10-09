from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from investing_monitor.domain.levels import PriceLevels
from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSnapshot,
    Position,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    RelativeAssessment,
    SituationAssessment,
    VolumeAssessment,
)
from investing_monitor.presentation.levels import (
    day_level_review,
    levels_text,
    position_text,
)
from investing_monitor.presentation.market_context import (
    pct_label,
    price_label,
    relative_outcome_line,
)
from investing_monitor.presentation.timing import session_label, timestamp, volume_basis


def volume_mood(ratio: float | None) -> str:
    """Characterful volume label instead of a dry number-only line."""
    if ratio is None:
        return "📊 거래량 판단 불가"
    if ratio < 0.6:
        return "💤 거래량 실종 — 참여자가 떠난 날"
    if ratio < 0.85:
        return "🪫 거래량 한산"
    if ratio <= 1.25:
        return "📊 거래량 평시 수준"
    if ratio < 1.5:
        return "📈 거래량 평시 상회"
    return "🔥 거래량 터짐"


def day_shape_text(
    close_price: float | None,
    day_low: float | None,
    day_high: float | None,
    reference_close: float | None,
    price_curve: Sequence[tuple[datetime, float]],
    volume_ratio: float | None,
) -> str:
    """One honest paragraph about how the session actually traded."""
    if close_price is None or day_low is None or day_high is None:
        return ""
    day_range = day_high - day_low
    close_position = (
        (close_price - day_low) / day_range if day_range > 0 else 0.5
    )
    phrases: list[str] = []
    if reference_close is not None and price_curve:
        open_gap = (price_curve[0][1] / reference_close - 1) * 100
        if open_gap >= 0.5:
            phrases.append(f"갭업 출발({pct_label(open_gap)})")
        elif open_gap <= -0.5:
            phrases.append(f"갭다운 출발({pct_label(open_gap)})")
        else:
            phrases.append("보합권 출발")
    high_zone = low_zone = None
    if len(price_curve) >= 6:
        closes = [value for _, value in price_curve]
        high_index = max(range(len(closes)), key=closes.__getitem__)
        low_index = min(range(len(closes)), key=closes.__getitem__)
        high_zone = _session_zone(high_index, len(closes))
        low_zone = _session_zone(low_index, len(closes))
        if high_zone in {"개장 초", "오전"} and close_position <= 0.35:
            phrases.append(
                f"{high_zone} 고점 {price_label(day_high)} 이후 흘러내림"
            )
        elif low_zone in {"개장 초", "오전"} and close_position >= 0.65:
            phrases.append(
                f"{low_zone} 저점 {price_label(day_low)} 찍고 꾸준히 회복"
            )
        elif close_position >= 0.65 and high_zone in {"오후", "마감 전"}:
            phrases.append(f"{high_zone} 고점 {price_label(day_high)} · 강하게 마감")
        elif close_position <= 0.35 and low_zone in {"오후", "마감 전"}:
            phrases.append(f"{low_zone}까지 저점을 낮추며 약세 마감")
        else:
            phrases.append("뚜렷한 방향 없이 당일 범위 안에서 횡보")
    phrases.append(f"종가는 당일 범위 하단에서 {close_position * 100:.0f}% 지점")
    first_line = " → ".join(phrases[:2]) + " · " + phrases[-1]

    verdict = _day_verdict(close_position, volume_ratio)
    volume_phrase = ""
    if volume_ratio is not None:
        volume_phrase = f"거래량 평시 {volume_ratio:.1f}배 — "
    return f"🗒️ *오늘의 요약*\n{first_line}\n{volume_phrase}{verdict}"


def _session_zone(index: int, total: int) -> str:
    fraction = index / max(1, total - 1)
    if fraction <= 0.2:
        return "개장 초"
    if fraction <= 0.5:
        return "오전"
    if fraction <= 0.8:
        return "오후"
    return "마감 전"


def _day_verdict(close_position: float, volume_ratio: float | None) -> str:
    quiet = volume_ratio is not None and volume_ratio < 0.85
    busy = volume_ratio is not None and volume_ratio >= 1.5
    if close_position <= 0.35:
        if quiet:
            return "매수세가 붙지 않은 무기력한 하루"
        if busy:
            return "거래량 실린 약세 — 매도 압력 확인 필요"
        return "힘없이 밀린 하루"
    if close_position >= 0.65:
        if busy:
            return "거래량 동반 강세 — 의미 있는 매수 유입"
        if quiet:
            return "가격은 버텼지만 거래량 없는 조용한 강세"
        return "매수 우위로 마감한 하루"
    if quiet:
        return "관망세 짙은 조용한 하루"
    return "공방 끝에 중립 마감"


def build_close_message(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
    situation: SituationAssessment | None = None,
    *,
    created_at: datetime | None = None,
    close_price: float | None = None,
    reference_close: float | None = None,
    day_low: float | None = None,
    day_high: float | None = None,
    levels: PriceLevels | None = None,
    position: Position | None = None,
    price_curve: Sequence[tuple[datetime, float]] = (),
) -> dict:
    direction_icon, direction_label = {
        Direction.UP: ("📈", "양전"),
        Direction.DOWN: ("📉", "음전"),
        Direction.FLAT: ("➖", "보합"),
    }[snapshot.direction]
    timing = (
        f"미국 거래일 {snapshot.trading_date:%m/%d} · {session_label(snapshot.session)}\n"
        f"시장 자료 {timestamp(snapshot.observed_at)}"
    )
    if created_at is not None:
        timing += f" · 브리프 작성 {timestamp(created_at)}"
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 ${snapshot.ticker} 장 마감 — {snapshot.trading_date:%m/%d}",
            },
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": timing}]},
        _section(
            f"{direction_icon} *종목 방향 · {direction_label}"
            + (f" ({pct_label(snapshot.change_pct)})" if close_price is not None else "")
            + "*"
        ),
    ]
    if close_price is not None:
        price_text = f"💵 *종가 {price_label(close_price)}*"
        if reference_close is not None:
            price_text += f" · 전일 {price_label(reference_close)}"
        if day_low is not None and day_high is not None:
            price_text += f"\n당일 범위 {price_label(day_low)} ~ {price_label(day_high)}"
        blocks.append(_section(price_text))

    shape = day_shape_text(
        close_price,
        day_low,
        day_high,
        reference_close,
        price_curve,
        volume_assessment.ratio,
    )
    if shape:
        blocks.append(_section(shape))

    relative_lines = [
        relative_outcome_line(
            f"반도체 지수({relative.benchmark_symbol})",
            relative.benchmark,
            relative.benchmark_strength,
        )
    ]
    benchmark_numbers = [f"{snapshot.ticker} {pct_label(snapshot.change_pct)}"]
    if snapshot.benchmark_change_pct is not None:
        benchmark_numbers.append(
            f"{relative.benchmark_symbol} {pct_label(snapshot.benchmark_change_pct)}"
        )
    relative_lines.append(" · ".join(benchmark_numbers))
    if relative.peers.value != "unavailable":
        peer_symbols = "·".join(relative.peer_symbols)
        relative_lines.append(
            relative_outcome_line(
                f"피어 평균({peer_symbols})", relative.peers, relative.peer_strength,
            )
        )
        if relative.peer_average_change_pct is not None:
            relative_lines.append(
                f"피어 평균 {pct_label(relative.peer_average_change_pct)}"
            )
    blocks.append(_section("\n".join(relative_lines)))

    if volume is not None and volume_assessment.is_ready:
        ratio = volume_assessment.ratio or 0.0
        blocks.append(
            _section(
                f"*{volume_mood(volume_assessment.ratio)}*\n{volume_basis(volume)}\n"
                f"당일 {volume.observed_volume:,}주 | "
                f"최근 {volume.baseline_sessions}거래일 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            )
        )

    if levels is not None:
        rendered_review = day_level_review(levels, close_price, day_low, day_high)
        if rendered_review:
            blocks.append(_section(rendered_review))
        rendered_levels = levels_text(levels, close_price)
        if rendered_levels:
            blocks.append(_section(rendered_levels))
    if position is not None:
        rendered_position = position_text(position, close_price)
        if rendered_position:
            blocks.append(_section(rendered_position))

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *오늘의 핵심 변화*"))
        blocks.extend(_section(_catalyst_text(catalyst)) for catalyst in selected)

    return {
        "text": (
            f"${snapshot.ticker} {snapshot.trading_date:%m/%d} 장 마감 브리프 "
            + (
                f"| {price_label(close_price)} ({pct_label(snapshot.change_pct)}) "
                if close_price is not None else ""
            )
            + f"| 자료 {timestamp(snapshot.observed_at)}"
            + (f" | 작성 {timestamp(created_at)}" if created_at else "")
        ),
        "blocks": blocks,
    }


def _catalyst_text(catalyst: Catalyst) -> str:
    label = "주요 이벤트"
    icon = "⚪"
    if catalyst.confidence.lower() == "high":
        icon, label = {
            ThesisImpact.STRENGTHEN: ("🟢", "논지 강화 근거"),
            ThesisImpact.NEUTRAL: ("⚪", "주요 이벤트"),
            ThesisImpact.RISK: ("🟠", "논지 위험 근거"),
            ThesisImpact.DAMAGE: ("🔴", "논지 훼손 근거"),
        }[catalyst.impact]
    return (
        f"{icon} *{label} · <{catalyst.source_url}|{_clip(catalyst.headline, 180)}>*\n"
        f"{_clip(catalyst.summary, 420)}\n"
        f"_{_clip(catalyst.source_name, 100)} · 발표 {timestamp(catalyst.published_at)}_"
    )


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"
