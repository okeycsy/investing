from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSnapshot,
    OfficialEvent,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import RelativeAssessment, VolumeAssessment
from investing_monitor.presentation.market_context import (
    pct_label,
    price_label,
    relative_detail_line,
    relative_outcome_line,
)


def build_weekly_message(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    strengthening: Sequence[Catalyst],
    risks: Sequence[Catalyst],
    upcoming_events: Sequence[OfficialEvent],
    *,
    period_start: date,
    period_end: date,
    session_count: int,
    last_close_price: float | None = None,
    week_low: float | None = None,
    week_high: float | None = None,
) -> dict:
    direction_icon, direction_label = {
        Direction.UP: ("📈", "상승"),
        Direction.DOWN: ("📉", "하락"),
        Direction.FLAT: ("➖", "보합"),
    }[snapshot.direction]
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": (
                    f"🧭 ${snapshot.ticker} 주간 논지 리뷰 · "
                    f"{period_start:%m/%d}–{period_end:%m/%d}"
                ),
            },
        },
        _context(f"완료된 정규장 {session_count}거래일 기준"),
        _section(
            f"{direction_icon} *주간 방향 · {direction_label} "
            f"({pct_label(snapshot.change_pct)})*"
        ),
    ]
    if last_close_price is not None:
        price_text = f"💵 *주간 마지막 종가 {price_label(last_close_price)}*"
        if week_low is not None and week_high is not None:
            price_text += (
                f" · 주간 범위 {price_label(week_low)} ~ {price_label(week_high)}"
                " (5분봉 종가 기준)"
            )
        blocks.append(_section(price_text))
    blocks.append(
        _section(
            relative_outcome_line(
                f"반도체 지수({relative.benchmark_symbol})",
                relative.benchmark,
                relative.benchmark_strength,
            )
            + f"\n{relative_detail_line(snapshot, relative)}"
        )
    )
    if relative.peers.value != "unavailable":
        peers = "·".join(relative.peer_symbols)
        blocks.append(
            _section(relative_outcome_line(f"피어 평균({peers})", relative.peers, relative.peer_strength))
        )

    if volume is not None and volume_assessment.is_ready:
        ratio = volume_assessment.ratio or 0.0
        status = (
            "🔥 최근 거래량 터짐"
            if volume_assessment.is_exploded
            else "📊 최근 거래량 평시 범위"
        )
        blocks.append(
            _section(
                f"*{status}*\n"
                f"마지막 거래일 {volume.observed_volume:,}주 | "
                f"최근 {volume.baseline_sessions}거래일 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            )
        )

    shown_strengthening = strengthening[: 1 if risks else 2]
    shown_risks = risks[: 1 if strengthening else 2]
    if shown_strengthening:
        blocks.append(_section("🟢 *이번 주 논지 강화 근거*"))
        blocks.extend(_section(_evidence_text(item)) for item in shown_strengthening)
    if shown_risks:
        blocks.append(_section("🔴 *이번 주 논지 위험 근거*"))
        blocks.extend(_section(_evidence_text(item)) for item in shown_risks)
    if not strengthening and not risks:
        blocks.append(_section("⚪ *이번 주 새로 확정된 논지 변화 없음*"))
    if upcoming_events:
        blocks.append(_section("📅 *다음 주 공식 일정*"))
        blocks.extend(_section(_event_text(event)) for event in upcoming_events[:3])

    return {
        "text": f"${snapshot.ticker} {period_end:%m/%d} 주간 논지 리뷰",
        "blocks": blocks,
    }


def _evidence_text(catalyst: Catalyst) -> str:
    details = _clip(catalyst.summary, 420)
    if catalyst.facts:
        details += f"\n• {_clip(catalyst.facts[0], 240)}"
    return (
        f"*<{catalyst.source_url}|{_clip(catalyst.headline, 180)}>*\n"
        f"{details}\n"
        f"_{_clip(catalyst.source_name, 100)}_"
    )


def _event_text(event: OfficialEvent) -> str:
    time_label = f" · {event.time_et} ET" if event.time_et else ""
    title = _clip(event.title_ko, 160)
    return f"• *<{event.source_url}|{event.event_date:%m/%d} {title}>*{time_label}"


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"
