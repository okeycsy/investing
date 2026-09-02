from __future__ import annotations

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
    context_parts = [candidate.source_name]
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


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


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
