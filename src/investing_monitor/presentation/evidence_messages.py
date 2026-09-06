from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from zoneinfo import ZoneInfo

from investing_monitor.domain.evidence import EvidenceAnalysis, EvidenceCandidate, EvidenceKind


KST = ZoneInfo("Asia/Seoul")


def build_evidence_message(
    candidate: EvidenceCandidate,
    analysis: EvidenceAnalysis,
) -> dict:
    if candidate.kind is EvidenceKind.INSIDER and candidate.metadata.get(
        "transaction_code"
    ):
        return _build_insider_message(candidate, analysis)
    icon, label = {
        EvidenceKind.NEWS: ("📰", "주요 회사 사건"),
        EvidenceKind.IR: ("📰", "주요 회사 발표"),
        EvidenceKind.SEC: ("🏛️", "중요 SEC 공시"),
        EvidenceKind.INSIDER: ("👤", "중요 내부자 거래"),
    }[candidate.kind]
    timestamp = candidate.published_at.astimezone(KST).strftime("%m/%d %H:%M KST")
    context_parts = [_source_tier_label(analysis.source_tier), candidate.source_name]
    form = str(candidate.metadata.get("form") or "")
    if candidate.kind is EvidenceKind.SEC and form:
        context_parts.append(form)
    context_parts.append(timestamp)
    impact_icon, impact_label = {
        "strengthen": ("🟢", "논지 강화 근거"),
        "neutral": ("⚪", "중립적 변화"),
        "risk": ("🟠", "주의 근거"),
        "damage": ("🔴", "논지 훼손 근거"),
    }[analysis.thesis_impact]
    facts = "\n".join(f"• {_clip(fact.fact_ko, 240)}" for fact in analysis.facts[:3])
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{icon} ${candidate.ticker} {label}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": " · ".join(context_parts),
                }
            ],
        },
        _section(
            f"*{_clip(analysis.headline_ko, 180)}*\n"
            f"{_clip(analysis.summary_ko, 650)}"
        ),
        _section(f"*확인된 사실*\n{facts}"),
        _section(
            f"{impact_icon} *{impact_label}*\n"
            f"{_clip(analysis.impact_reason_ko, 300)}"
        ),
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"<{candidate.source_url}|"
                        f"{'SEC 원문 보기' if candidate.kind is EvidenceKind.SEC else '원문 보기'}>"
                    ),
                }
            ],
        },
    ]
    return {
        "text": f"${candidate.ticker} {analysis.headline_ko}",
        "blocks": blocks,
    }


def build_move_followup_message(
    parent_context: Mapping[str, object],
    parent_created_at: datetime,
    candidate: EvidenceCandidate,
    analysis: EvidenceAnalysis,
) -> dict:
    direction = str(parent_context.get("direction") or "")
    level = int(parent_context.get("level") or 0)
    signed_level = level if direction == "up" else -level
    direction_label = "상승" if direction == "up" else "하락"
    icon = "📈" if direction == "up" else "📉"
    move_at = parent_created_at.astimezone(KST).strftime("%m/%d %H:%M KST")
    evidence_at = candidate.published_at.astimezone(KST).strftime("%m/%d %H:%M KST")
    facts = "\n".join(
        f"• {_clip(fact.fact_ko, 240)}" for fact in analysis.facts[:2]
    )
    relationship = _move_relationship(direction, analysis.thesis_impact)
    return {
        "text": (
            f"${candidate.ticker} {signed_level:+.1f}% "
            f"{direction_label} 움직임 후속 확인"
        ),
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": (
                        f"🔎 ${candidate.ticker} {signed_level:+.1f}% "
                        f"움직임 후속 확인"
                    ),
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"{icon} 가격 구간 {move_at} · "
                            f"근거 게시 {evidence_at}"
                        ),
                    }
                ],
            },
            _section(
                f"*새로 확인된 근거*\n"
                f"*{_clip(analysis.headline_ko, 180)}*\n"
                f"{_clip(analysis.summary_ko, 560)}"
            ),
            _section(f"*확인된 사실*\n{facts}"),
            _section(f"*움직임과의 관계*\n{relationship}"),
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"{_source_tier_label(analysis.source_tier)} · "
                            f"<{candidate.source_url}|{candidate.source_name} 원문 보기>"
                        ),
                    }
                ],
            },
        ],
    }


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _source_tier_label(value: str) -> str:
    return {
        "official": "공식 원문",
        "primary_reporting": "주요 원보도",
        "secondary": "2차 분석",
    }.get(value, "출처 확인")


def _move_relationship(direction: str, thesis_impact: str) -> str:
    aligned = (direction == "up" and thesis_impact == "strengthen") or (
        direction == "down" and thesis_impact in {"risk", "damage"}
    )
    opposed = (direction == "up" and thesis_impact in {"risk", "damage"}) or (
        direction == "down" and thesis_impact == "strengthen"
    )
    if aligned:
        return (
            "움직임 방향과 일치하는 회사 근거가 시간상 인접해 확인됨. "
            "다만 인과관계가 확정된 것은 아님."
        )
    if opposed:
        return (
            "움직임 방향과 근거의 성격이 엇갈려 단독 원인으로 보기 어려움. "
            "인과관계가 확정된 것은 아님."
        )
    return (
        "가격 움직임과 가까운 시점에 확인된 회사 근거임. "
        "시간상 인접성만으로 인과관계가 확정된 것은 아님."
    )


def _build_insider_message(
    candidate: EvidenceCandidate,
    analysis: EvidenceAnalysis,
) -> dict:
    code = str(candidate.metadata.get("transaction_code") or "").upper()
    icon, label = {
        "P": ("🟢", "장내 매수"),
        "S": ("🔴", "장내 매도"),
    }.get(code, ("⚪", "내부자 거래"))
    timestamp = candidate.published_at.astimezone(KST).strftime("%m/%d KST")
    name = _clip(str(candidate.metadata.get("insider_name") or "내부자"), 120)
    position = _clip(str(candidate.metadata.get("position") or "직책 미상"), 120)
    shares = int(float(candidate.metadata.get("shares") or 0))
    value_usd = float(candidate.metadata.get("value_usd") or 0)
    scale = f"{shares:,}주"
    if value_usd > 0:
        scale += f" · 신고 금액 약 ${value_usd:,.0f}"
    original_label = (
        "SEC Form 4 원문"
        if candidate.metadata.get("exact_form")
        else "Yahoo Finance 집계 보기"
    )
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"👤 ${candidate.ticker} 중요 내부자 거래",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{candidate.source_name} · {timestamp}",
                }
            ],
        },
        _section(f"{icon} *{label}*\n*{name}* · {position}"),
        _section(f"*거래 규모*\n{scale}"),
        _section(
            "*거래 성격*\n"
            "보상·옵션 행사·세금 처리와 구분되는 공개시장 거래"
        ),
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"<{candidate.source_url}|{original_label}>",
                }
            ],
        },
    ]
    return {
        "text": f"${candidate.ticker} {analysis.headline_ko}",
        "blocks": blocks,
    }
