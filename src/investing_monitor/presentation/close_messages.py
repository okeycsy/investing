from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSnapshot,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    RelativeAssessment,
    SituationAssessment,
    VolumeAssessment,
)
from investing_monitor.presentation.market_context import relative_outcome_line, situation_text
from investing_monitor.presentation.timing import session_label, timestamp, volume_basis


def build_close_message(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
    situation: SituationAssessment | None = None,
    *,
    created_at: datetime | None = None,
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
        _section(f"{direction_icon} *종목 방향 · {direction_label}*"),
    ]
    blocks.append(
        _section(
            relative_outcome_line(
                f"반도체 지수({relative.benchmark_symbol})",
                relative.benchmark,
                relative.benchmark_strength,
            )
        )
    )

    if relative.peers.value != "unavailable":
        peer_symbols = "·".join(relative.peer_symbols)
        blocks.append(
            _section(
                relative_outcome_line(
                    f"피어 평균({peer_symbols})", relative.peers, relative.peer_strength,
                )
            )
        )
    if situation is not None:
        blocks.append(_section(situation_text(situation, snapshot.direction)))

    if volume is not None and volume_assessment.is_ready:
        ratio = volume_assessment.ratio or 0.0
        status = (
            "🔥 거래량 터짐"
            if volume_assessment.is_exploded
            else "📊 거래량 평시 범위"
        )
        blocks.append(
            _section(
                f"*{status}*\n{volume_basis(volume)}\n"
                f"당일 {volume.observed_volume:,}주 | "
                f"최근 {volume.baseline_sessions}거래일 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            )
        )

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *오늘의 핵심 변화*"))
        blocks.extend(_section(_catalyst_text(catalyst)) for catalyst in selected)

    return {
        "text": (
            f"${snapshot.ticker} {snapshot.trading_date:%m/%d} 장 마감 브리프 "
            f"| 자료 {timestamp(snapshot.observed_at)}"
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
