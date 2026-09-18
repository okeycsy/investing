from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime

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
from investing_monitor.presentation.market_context import (
    delta_text,
    relative_outcome_line,
    situation_text,
)
from investing_monitor.presentation.timing import (
    DELAYED_DETECTION_SECONDS,
    delay_seconds,
    duration_label,
    observation_context,
    session_label,
    timestamp,
    volume_basis,
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
) -> dict:
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
            observation_context(
                signal.observed_at,
                session_label(signal.session),
                detected_at,
                detection_delay_seconds,
            )
        ),
    ]

    # Historical moves cannot borrow a later volume reading as event-time context.
    rendered_delta = delta_text(replace(delta, volume=None) if historical and delta else delta)
    latest_status = ""
    if historical:
        if latest_snapshot is not None:
            latest_status = _band_status(signal, latest_snapshot)
            blocks.append(_section(
                f"*마지막 관측 · {session_label(latest_snapshot.session)} "
                f"{timestamp(latest_snapshot.observed_at)}*\n"
                f"{_direction_text(latest_snapshot)} · {latest_status}"
            ))
            source_age = delay_seconds(latest_snapshot.observed_at, detected_at)
            if source_age > DELAYED_DETECTION_SECONDS:
                blocks.append(_context(
                    f"마지막 자료도 봇 확인보다 {duration_label(source_age)} 이전 · "
                    "이후 상태는 확인되지 않음"
                ))
            if latest_relative is not None:
                blocks.append(_section(_relative_text(latest_relative)))
            else:
                blocks.append(_section(
                    f"반도체 지수({relative.benchmark_symbol}) · 마지막 관측 비교 자료 없음"
                ))
            if latest_situation is not None:
                blocks.append(_section(situation_text(latest_situation, latest_snapshot.direction)))
        else:
            blocks.append(_context("이후 관측 자료 없음 · 아래 상대 흐름은 도달 당시 기준"))
            blocks.append(_section(_relative_text(relative)))
            if situation is not None:
                blocks.append(_section(situation_text(situation, signal.direction)))
        if rendered_delta:
            blocks.append(_section(f"*도달 당시 · 이전 도달 기록과 비교*\n{rendered_delta}"))
    else:
        if rendered_delta:
            blocks.append(_section(rendered_delta))
        blocks.append(_section(_relative_text(relative)))
        if situation is not None:
            blocks.append(_section(situation_text(situation, signal.direction)))

    if volume is not None and volume_assessment.is_ready:
        status = "🔥 거래량 확대" if volume_assessment.is_exploded else "거래량 평시 범위"
        blocks.append(
            _section(
                f"*{status}*\n{volume_basis(volume)}\n"
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

    fallback = f"{title} | 관측 {timestamp(signal.observed_at)}"
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
) -> dict:
    observed_at = volume.observed_at or signal.observed_at
    if detected_at is not None:
        detection_delay_seconds = delay_seconds(observed_at, detected_at)
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
    delayed = detection_delay_seconds > DELAYED_DETECTION_SECONDS
    title = f"${signal.ticker} 거래량 {ratio:.1f}배 확대"
    if delayed:
        title += " · 지연 확인"
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔥 {title}",
            },
        },
        _context(
            observation_context(
                observed_at,
                "거래량 기준 시각",
                detected_at,
                detection_delay_seconds,
            )
        ),
    ]
    blocks.extend(
        [
            _section(
                f"*동시간대 거래량 터짐*\n{volume_basis(volume)}\n"
                f"누적 {volume.observed_volume:,}주 | "
                f"과거 {volume.baseline_sessions}거래일 동시간 평균 "
                f"{volume.expected_volume:,}주 | {ratio:.1f}배"
            ),
            _section(
                f"*시장 관측 · {session_label(snapshot.session)} {timestamp(snapshot.observed_at)}*\n"
                f"{direction_icon} *종목 방향: {direction_label}*\n{_relative_text(relative)}"
            ),
        ]
    )
    if situation is not None:
        blocks.append(_section(situation_text(situation, snapshot.direction)))
    fallback = f"{title} | 자료 {timestamp(observed_at)}"
    if detected_at is not None:
        fallback += f" | 확인 {timestamp(detected_at)}"
    return {
        "text": fallback,
        "blocks": blocks,
    }


def _relative_text(relative: RelativeAssessment) -> str:
    benchmark_label = f"반도체 지수({relative.benchmark_symbol})"
    lines = [
        relative_outcome_line(benchmark_label, relative.benchmark, relative.benchmark_strength)
    ]
    if relative.peers.value != "unavailable":
        peer_label = f"피어({'·'.join(relative.peer_symbols)})"
        lines.append(relative_outcome_line(peer_label, relative.peers, relative.peer_strength))
    return "\n".join(lines)


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
