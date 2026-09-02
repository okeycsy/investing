from __future__ import annotations

from collections.abc import Sequence

from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSnapshot,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import RelativeAssessment, VolumeAssessment


def build_close_message(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
) -> dict:
    direction_icon, direction_label = {
        Direction.UP: ("📈", "양전"),
        Direction.DOWN: ("📉", "음전"),
        Direction.FLAT: ("➖", "보합"),
    }[snapshot.direction]
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 ${snapshot.ticker} 장 마감 — {snapshot.trading_date:%m/%d}",
            },
        },
        _section(f"{direction_icon} *종목 방향 · {direction_label}*"),
        _section(
            _outcome_line(
                f"반도체 지수({relative.benchmark_symbol})",
                relative.benchmark.value,
            )
        ),
    ]

    if relative.peers.value != "unavailable":
        peer_symbols = "·".join(relative.peer_symbols)
        blocks.append(
            _section(_outcome_line(f"피어 평균({peer_symbols})", relative.peers.value))
        )

    if volume is not None and volume_assessment.is_ready:
        ratio = volume_assessment.ratio or 0.0
        status = (
            "🔥 거래량 터짐"
            if volume_assessment.is_exploded
            else "📊 거래량 평시 범위"
        )
        blocks.append(
            _section(
                f"*{status}*\n"
                f"당일 {volume.observed_volume:,}주 | "
                f"최근 {volume.lookback_sessions}거래일 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            )
        )

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *오늘의 핵심 변화*"))
        blocks.extend(_section(_catalyst_text(catalyst)) for catalyst in selected)

    return {
        "text": f"${snapshot.ticker} {snapshot.trading_date:%m/%d} 장 마감 브리프",
        "blocks": blocks,
    }


def _outcome_line(label: str, outcome: str) -> str:
    icon, text = {
        "outperform": ("↗️", "아웃퍼폼"),
        "underperform": ("↘️", "언더퍼폼"),
        "inline": ("↔️", "비슷한 흐름"),
        "unavailable": ("⚪", "비교 불가"),
    }[outcome]
    return f"{icon} *{label} 대비 {text}*"


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
        f"_{_clip(catalyst.source_name, 100)}_"
    )


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"
