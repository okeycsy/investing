from __future__ import annotations

from collections.abc import Sequence
from zoneinfo import ZoneInfo

from investing_monitor.domain.models import Catalyst, Direction, PriceBandSignal, VolumeSnapshot
from investing_monitor.domain.policies import RelativeAssessment, VolumeAssessment


KST = ZoneInfo("Asia/Seoul")


def build_price_band_message(
    signal: PriceBandSignal,
    relative: RelativeAssessment,
    volume: VolumeSnapshot | None,
    volume_assessment: VolumeAssessment,
    catalysts: Sequence[Catalyst],
) -> dict:
    direction_icon = "📈" if signal.direction is Direction.UP else "📉"
    direction_label = "상승" if signal.direction is Direction.UP else "하락"
    signed_level = signal.level if signal.direction is Direction.UP else -signal.level
    reversal = " · 장중 방향 반전" if signal.is_reversal else ""
    timestamp = signal.observed_at.astimezone(KST).strftime("%m/%d %H:%M KST")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": (
                    f"{direction_icon} ${signal.ticker} {signed_level:+d}% "
                    f"{direction_label} 구간 진입{reversal}"
                ),
            },
        },
        _context(timestamp),
        _section(_relative_text(relative)),
    ]

    if volume is not None and volume_assessment.is_ready:
        status = "🔥 거래량 동반" if volume_assessment.is_exploded else "거래량은 아직 평시 범위"
        blocks.append(
            _section(
                f"*{status}*\n"
                f"누적 {volume.observed_volume:,}주 | "
                f"동시간대 {volume.lookback_sessions}일 평균 {volume.expected_volume:,}주 | "
                f"{volume_assessment.ratio:.1f}배"
            )
        )

    selected = list(catalysts[:2])
    if selected:
        blocks.append(_section("*무슨 일이 있었나*"))
        for catalyst in selected:
            blocks.append(_section(_catalyst_text(catalyst)))
    else:
        blocks.append(
            _context(
                "확인된 직접 촉매 없음. 시장 수급 또는 아직 보도되지 않은 종목 고유 요인 가능."
            )
        )

    return {
        "text": f"${signal.ticker} {signed_level:+d}% {direction_label} 구간 진입",
        "blocks": blocks,
    }


def _relative_text(relative: RelativeAssessment) -> str:
    lines = [_outcome_line("반도체 지수", relative.benchmark)]
    if relative.peer_symbols:
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


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}
