from __future__ import annotations

from dataclasses import dataclass

from investing_monitor.domain.evidence import (
    EvidenceAnalysis,
    EvidenceCandidate,
    GroundedFact,
)


PURCHASE_MINIMUM_USD = 100_000
SALE_MINIMUM_USD = 1_000_000
HOLDING_CHANGE_MINIMUM = 0.20

TRANSACTION_LABELS = {
    "P": "장내 매수",
    "S": "장내 매도",
    "A": "주식 보상",
    "M": "옵션 행사",
    "F": "세금 원천징수",
}


@dataclass(frozen=True)
class InsiderMateriality:
    material: bool
    reason: str


def assess_insider_materiality(candidate: EvidenceCandidate) -> InsiderMateriality | None:
    raw_codes = candidate.metadata.get("transaction_codes") or (
        candidate.metadata.get("transaction_code"),
    )
    codes = {
        str(code).upper().strip()
        for code in raw_codes
        if str(code or "").strip()
    }
    if not codes:
        return None
    directional = codes & {"P", "S"}
    if not directional:
        labels = ", ".join(TRANSACTION_LABELS.get(code, code) for code in sorted(codes))
        return InsiderMateriality(False, f"non-directional insider event: {labels}")

    value_usd = _number(candidate.metadata.get("value_usd"))
    holding_change = _number(candidate.metadata.get("holding_change_ratio"))
    if "P" in directional and value_usd >= PURCHASE_MINIMUM_USD:
        return InsiderMateriality(True, "open-market purchase above $100k")
    if "S" in directional and (
        value_usd >= SALE_MINIMUM_USD
        or holding_change >= HOLDING_CHANGE_MINIMUM
    ):
        return InsiderMateriality(True, "material open-market sale")
    return InsiderMateriality(False, "directional transaction below materiality threshold")


def build_insider_analysis(candidate: EvidenceCandidate) -> EvidenceAnalysis:
    code = str(candidate.metadata.get("transaction_code") or "").upper()
    label = TRANSACTION_LABELS.get(code, "내부자 거래")
    name = str(candidate.metadata.get("insider_name") or "내부자")
    position = str(candidate.metadata.get("position") or "직책 미상")
    shares = int(_number(candidate.metadata.get("shares")))
    value_usd = _number(candidate.metadata.get("value_usd"))
    detail = f"{shares:,}주"
    if value_usd > 0:
        detail += f", 신고 금액 약 ${value_usd:,.0f}"
    fact = f"{name} ({position})의 {label} {detail}가 보고됐다."
    return EvidenceAnalysis(
        candidate_id=candidate.candidate_id,
        relevant=True,
        headline_ko=f"{name}, {label} {shares:,}주",
        summary_ko=fact,
        facts=(GroundedFact(candidate.source_text, fact),),
        interpretation_ko="보상·세금 처리와 구분되는 공개시장 거래다.",
        thesis_impact="neutral",
        impact_reason_ko="내부자 거래 하나만으로 투자 논지 변화를 단정하지 않는다.",
        confidence="high",
    )


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
