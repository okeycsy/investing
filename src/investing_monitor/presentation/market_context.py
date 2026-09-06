from __future__ import annotations

from investing_monitor.domain.models import Direction, SituationVerdict
from investing_monitor.domain.policies import SituationAssessment
from investing_monitor.domain.situation import MarketContextDelta


def situation_text(
    assessment: SituationAssessment,
    direction: Direction,
) -> str:
    if assessment.verdict is SituationVerdict.COMPANY_STRENGTH:
        label = _company_signal_label(assessment, "강세")
        detail = _expectation_detail(
            assessment,
            "반도체·피어 흐름을 함께 웃돎",
        )
        return f"🧭 *오늘의 판정 · {label}*\n{detail}"
    if assessment.verdict is SituationVerdict.COMPANY_WEAKNESS:
        label = _company_signal_label(assessment, "약세")
        detail = _expectation_detail(
            assessment,
            "반도체·피어 흐름을 함께 밑돎",
        )
        return f"🧭 *오늘의 판정 · {label}*\n{detail}"
    if assessment.verdict is SituationVerdict.BROADLY_EXPLAINED:
        move = {
            Direction.UP: "상승",
            Direction.DOWN: "하락",
            Direction.FLAT: "보합",
        }[direction]
        return (
            f"🌐 *오늘의 판정 · 시장·피어 흐름으로 설명되는 {move}*\n"
            "현재 상대 흐름만으로 종목 고유 사건을 단정하기 어려움"
        )
    if assessment.verdict is SituationVerdict.MIXED:
        return (
            "🧭 *오늘의 판정 · 상대 흐름 혼합*\n"
            "종목 고유 움직임인지 추가 근거 확인 필요"
        )
    return (
        "⚪ *오늘의 판정 · 판단 보류*\n"
        "반도체 또는 피어 데이터가 부족해 상대 맥락을 확정하지 않음"
    )


def delta_text(delta: MarketContextDelta | None) -> str:
    if delta is None:
        return ""
    lines = []
    if delta.situation:
        lines.append(
            f"• 상황 판정: {_situation_label(delta.situation[0])} → "
            f"{_situation_label(delta.situation[1])}"
        )
    if delta.benchmark:
        lines.append(
            f"• 반도체 상대 흐름: {_outcome_label(delta.benchmark[0])} → "
            f"{_outcome_label(delta.benchmark[1])}"
        )
    if delta.peers:
        lines.append(
            f"• 피어 상대 흐름: {_outcome_label(delta.peers[0])} → "
            f"{_outcome_label(delta.peers[1])}"
        )
    if delta.volume:
        lines.append(
            f"• 거래량: {_volume_label(delta.volume[0])} → "
            f"{_volume_label(delta.volume[1])}"
        )
    if delta.new_catalyst_count:
        lines.append(f"• 새로 확인된 관련 사건 {delta.new_catalyst_count}건")
    if not lines:
        lines.append("• 핵심 맥락 유지 · 새 가격 구간만 진입")
    return "*직전 가격 알림 이후*\n" + "\n".join(lines)


def _expectation_detail(
    assessment: SituationAssessment,
    fallback: str,
) -> str:
    if assessment.sensitivity_adjusted:
        direction = "웃돎" if "웃돎" in fallback else "밑돎"
        return f"최근 6개월 통상 민감도 기준 기대 범위를 함께 {direction}"
    return fallback


def _company_signal_label(
    assessment: SituationAssessment,
    direction: str,
) -> str:
    confidence = "높음" if assessment.confidence == "high" else "있음"
    return f"종목 고유 {direction} 가능성 {confidence}"


def _outcome_label(value: str) -> str:
    return {
        "outperform": "아웃퍼폼",
        "underperform": "언더퍼폼",
        "inline": "동조",
        "unavailable": "비교 불가",
    }.get(value, value)


def _situation_label(value: str) -> str:
    return {
        "company_strength": "종목 고유 강세",
        "company_weakness": "종목 고유 약세",
        "broadly_explained": "시장·피어 설명 범위",
        "mixed": "혼합",
        "unavailable": "판단 보류",
    }.get(value, value)


def _volume_label(value: str) -> str:
    return {
        "exploded": "평시 범위 초과",
        "normal": "평시 범위",
        "unavailable": "판단 불가",
    }.get(value, value)
