from __future__ import annotations

from collections.abc import Sequence
from zoneinfo import ZoneInfo

from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSnapshot,
    PriceBandSignal,
    VolumeSignal,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    RelativeAssessment,
    SituationAssessment,
    VolumeAssessment,
)
from investing_monitor.domain.situation import MarketContextDelta
from investing_monitor.presentation.market_context import delta_text, situation_text


KST = ZoneInfo("Asia/Seoul")


def build_price_band_message(
    signal: PriceBandSignal,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
    *,
    detection_delay_seconds: int = 0,
    situation: SituationAssessment | None = None,
    delta: MarketContextDelta | None = None,
) -> dict:
    direction_icon = "📈" if signal.direction is Direction.UP else "📉"
    direction_label = "상승" if signal.direction is Direction.UP else "하락"
    signed_level = signal.level if signal.direction is Direction.UP else -signal.level
    reversal = " · 장중 방향 반전" if signal.is_reversal else ""
    timestamp = signal.observed_at.astimezone(KST).strftime("%m/%d %H:%M KST")
    session_label = {
        "pre": "프리마켓",
        "regular": "정규장",
        "post": "애프터마켓",
        "closed": "장외",
    }[signal.session.value]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": (
                    f"{direction_icon} ${signal.ticker} {signed_level:+.1f}% "
                    f"{direction_label} 구간 진입{reversal}"
                ),
            },
        },
        _context(
            _detection_context(
                session_label,
                timestamp,
                detection_delay_seconds,
            )
        ),
    ]

    if situation is not None:
        blocks.append(_section(situation_text(situation, signal.direction)))
    rendered_delta = delta_text(delta)
    if rendered_delta:
        blocks.append(_section(rendered_delta))
    blocks.append(_section(_relative_text(relative)))

    if volume is not None and volume_assessment.is_ready:
        status = "🔥 거래량 동반" if volume_assessment.is_exploded else "거래량은 아직 평시 범위"
        blocks.append(
            _section(
                f"*{status}*\n"
                f"누적 {volume.observed_volume:,}주 | "
                f"동시간대 {volume.baseline_sessions}거래일 평균 "
                f"{volume.expected_volume:,}주 | "
                f"{volume_assessment.ratio:.1f}배"
            )
        )

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("📰 *최근 확인된 관련 사건*"))
        for catalyst in selected:
            blocks.append(_section(_catalyst_text(catalyst)))
        blocks.append(
            _context("발표 시각이 인접한 근거이며 주가 움직임의 인과관계를 단정하지 않음")
        )
    else:
        blocks.append(
            _section(
                "🔎 *직접 촉매 아직 확인되지 않음*\n"
                "시장 수급 또는 아직 보도되지 않은 종목 고유 요인일 수 있음"
            )
        )

    return {
        "text": f"${signal.ticker} {signed_level:+.1f}% {direction_label} 구간 진입",
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
    situation: SituationAssessment | None = None,
) -> dict:
    timestamp = signal.observed_at.astimezone(KST).strftime("%m/%d %H:%M KST")
    direction_icon = {
        Direction.UP: "📈",
        Direction.DOWN: "📉",
        Direction.FLAT: "↔️",
    }[snapshot.direction]
    direction_label = {
        Direction.UP: "양전",
        Direction.DOWN: "음전",
        Direction.FLAT: "보합",
    }[snapshot.direction]
    ratio = volume_assessment.ratio or 0.0
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔥 ${signal.ticker} 거래량 {ratio:.1f}배 확대",
            },
        },
        _context(
            _detection_context(
                "거래량 기준 시각",
                timestamp,
                detection_delay_seconds,
            )
        ),
    ]
    if situation is not None:
        blocks.append(_section(situation_text(situation, snapshot.direction)))
    blocks.extend(
        [
            _section(
                f"*동시간대 거래량 터짐*\n"
                f"누적 {volume.observed_volume:,}주 | "
                f"과거 {volume.baseline_sessions}거래일 동시간 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            ),
            _section(f"{direction_icon} *종목 방향: {direction_label}*\n{_relative_text(relative)}"),
        ]
    )
    return {
        "text": f"${signal.ticker} 거래량 {ratio:.1f}배 확대",
        "blocks": blocks,
    }


def _relative_text(relative: RelativeAssessment) -> str:
    benchmark_label = f"반도체 지수({relative.benchmark_symbol})"
    lines = [_outcome_line(benchmark_label, relative.benchmark)]
    if relative.peers.value != "unavailable":
        peer_label = f"피어({'·'.join(relative.peer_symbols)})"
        lines.append(_outcome_line(peer_label, relative.peers))
    return "\n".join(lines)


def _outcome_line(label: str, outcome) -> str:
    mapping = {
        "outperform": ("↗️", "아웃퍼폼"),
        "underperform": ("↘️", "언더퍼폼"),
        "inline": ("↔️", "동조"),
        "unavailable": ("", "비교 불가"),
    }
    icon, text = mapping[outcome.value]
    return f"{icon} *{label} 대비 {text}*".strip()


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
        f"_{catalyst.source_name}_"
    )


def _detection_context(
    session_label: str,
    timestamp: str,
    detection_delay_seconds: int,
) -> str:
    if detection_delay_seconds <= 10 * 60:
        return f"{session_label} · {timestamp}"
    return (
        f"⏱️ 지연 감지 · {session_label} {timestamp} 발생 · "
        f"{_duration_label(detection_delay_seconds)} 뒤 복구"
    )


def _duration_label(seconds: int) -> str:
    minutes = max(1, round(seconds / 60))
    hours, remaining = divmod(minutes, 60)
    if not hours:
        return f"{remaining}분"
    if not remaining:
        return f"{hours}시간"
    return f"{hours}시간 {remaining}분"


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}
