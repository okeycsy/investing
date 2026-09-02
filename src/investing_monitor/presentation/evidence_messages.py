from __future__ import annotations

from zoneinfo import ZoneInfo

from investing_monitor.domain.evidence import EvidenceAnalysis, EvidenceCandidate, EvidenceKind


KST = ZoneInfo("Asia/Seoul")


def build_evidence_message(
    candidate: EvidenceCandidate,
    analysis: EvidenceAnalysis,
) -> dict:
    icon, label = {
        EvidenceKind.NEWS: ("📰", "주요 회사 사건"),
        EvidenceKind.IR: ("📰", "주요 회사 발표"),
        EvidenceKind.SEC: ("🏛️", "중요 SEC 공시"),
        EvidenceKind.INSIDER: ("👤", "중요 내부자 거래"),
    }[candidate.kind]
    timestamp = candidate.published_at.astimezone(KST).strftime("%m/%d %H:%M KST")
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
                    "text": f"{candidate.source_name} · {timestamp}",
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
            f"{_clip(analysis.interpretation_ko, 400)}\n"
            f"{_clip(analysis.impact_reason_ko, 300)}"
        ),
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"<{candidate.source_url}|원문 보기>",
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
