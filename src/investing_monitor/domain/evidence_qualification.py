from __future__ import annotations

from typing import Mapping

from investing_monitor.domain.evidence import (
    EvidenceAnalysis,
    EvidenceCandidate,
    EvidenceDisposition,
    EvidenceEventType,
    EvidenceKind,
    EvidenceMateriality,
    EvidenceSourceTier,
)


IMMEDIATE_EVENT_TYPES = {
    EvidenceEventType.ACQUISITION.value,
    EvidenceEventType.EARNINGS.value,
    EvidenceEventType.GUIDANCE.value,
    EvidenceEventType.MAJOR_CONTRACT.value,
    EvidenceEventType.MAJOR_CUSTOMER.value,
    EvidenceEventType.MANAGEMENT.value,
    EvidenceEventType.REGULATORY.value,
}

BRIEFING_EVENT_TYPES = IMMEDIATE_EVENT_TYPES | {
    EvidenceEventType.FINANCING.value,
    EvidenceEventType.CAPACITY.value,
    EvidenceEventType.PRODUCT.value,
    EvidenceEventType.PARTNERSHIP.value,
}


def evidence_disposition(
    kind: EvidenceKind,
    analysis: EvidenceAnalysis,
) -> EvidenceDisposition:
    if not analysis.relevant:
        return EvidenceDisposition.LEDGER
    if kind in {EvidenceKind.SEC, EvidenceKind.INSIDER}:
        return EvidenceDisposition.IMMEDIATE
    if (
        not analysis.company_directness
        or not analysis.new_fact
        or analysis.confidence == "low"
    ):
        return EvidenceDisposition.LEDGER
    if (
        analysis.alert_worthy
        and analysis.confidence == "high"
        and analysis.materiality == EvidenceMateriality.HIGH.value
        and analysis.event_type in IMMEDIATE_EVENT_TYPES
    ):
        return EvidenceDisposition.IMMEDIATE
    if (
        analysis.materiality
        in {EvidenceMateriality.HIGH.value, EvidenceMateriality.MEDIUM.value}
        and analysis.event_type in BRIEFING_EVENT_TYPES
    ):
        return EvidenceDisposition.BRIEFING
    return EvidenceDisposition.LEDGER


def evidence_source_tier(candidate: EvidenceCandidate) -> EvidenceSourceTier:
    if candidate.kind in {EvidenceKind.IR, EvidenceKind.SEC, EvidenceKind.INSIDER}:
        return EvidenceSourceTier.OFFICIAL
    source = candidate.source_name.casefold()
    if any(
        publisher in source
        for publisher in (
            "reuters",
            "bloomberg",
            "associated press",
            "dow jones",
            "financial times",
            "wall street journal",
            "cnbc",
        )
    ):
        return EvidenceSourceTier.PRIMARY_REPORTING
    if any(
        distributor in source
        for distributor in ("pr newswire", "business wire", "globenewswire")
    ):
        return EvidenceSourceTier.OFFICIAL
    return EvidenceSourceTier.SECONDARY


def legacy_evidence_qualification(payload: Mapping[str, object]) -> dict[str, object]:
    if any(
        key in payload
        for key in (
            "event_type",
            "company_directness",
            "new_fact",
            "materiality",
            "alert_worthy",
        )
    ):
        return {
            "event_type": EvidenceEventType.OTHER.value,
            "company_directness": False,
            "new_fact": False,
            "materiality": EvidenceMateriality.LOW.value,
            "alert_worthy": False,
        }
    text_parts = [
        str(payload.get("headline_ko") or ""),
        str(payload.get("summary_ko") or ""),
        str(payload.get("impact_reason_ko") or ""),
    ]
    text_parts.extend(
        str(fact.get("fact_ko") or "")
        for fact in payload.get("facts") or []
        if isinstance(fact, dict)
    )
    text = " ".join(text_parts).casefold()
    event_type = _legacy_event_type(text)
    confidence = str(payload.get("confidence") or "medium")
    direct = bool(payload.get("relevant")) and bool(text)
    high = event_type in IMMEDIATE_EVENT_TYPES
    briefing = event_type in BRIEFING_EVENT_TYPES - IMMEDIATE_EVENT_TYPES
    return {
        "event_type": event_type,
        "company_directness": direct,
        "new_fact": direct,
        "materiality": (
            EvidenceMateriality.HIGH.value
            if high
            else EvidenceMateriality.MEDIUM.value
            if briefing
            else EvidenceMateriality.LOW.value
        ),
        "alert_worthy": direct and high and confidence == "high",
    }


def _legacy_event_type(text: str) -> str:
    event_patterns = (
        (
            EvidenceEventType.ACQUISITION.value,
            ("인수", "합병", "acquisition", "acquire", "merger"),
        ),
        (EvidenceEventType.GUIDANCE.value, ("가이던스", "연간 전망", "guidance")),
        (
            EvidenceEventType.EARNINGS.value,
            ("실적", "분기 보고", "earnings", "quarterly results"),
        ),
        (
            EvidenceEventType.MAJOR_CUSTOMER.value,
            ("주요 고객", "대형 고객", "major customer"),
        ),
        (
            EvidenceEventType.MAJOR_CONTRACT.value,
            ("수주", "중대 계약", "major contract"),
        ),
        (
            EvidenceEventType.MANAGEMENT.value,
            ("최고경영자", "최고재무책임자", "경영진", " ceo ", " cfo "),
        ),
        (
            EvidenceEventType.REGULATORY.value,
            ("규제", "당국 조사", "regulatory", "antitrust"),
        ),
        (EvidenceEventType.CAPACITY.value, ("생산능력", "생산 능력", "capacity")),
        (EvidenceEventType.PRODUCT.value, ("신제품", "제품 출시", "new product")),
        (EvidenceEventType.PARTNERSHIP.value, ("파트너십", "협력", "partnership")),
        (EvidenceEventType.FINANCING.value, ("자금조달", "채권 발행", "financing")),
    )
    for event_type, patterns in event_patterns:
        if any(pattern in f" {text} " for pattern in patterns):
            return event_type
    return EvidenceEventType.COMMENTARY.value
