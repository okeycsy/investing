from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date

from investing_monitor.adapters.anthropic_evidence import EvidenceAnalysisBatch
from investing_monitor.domain.evidence import (
    EvidenceAnalysis,
    EvidenceCandidate,
    EvidenceEventType,
    EvidenceKind,
    EvidenceMateriality,
    EvidenceProfile,
    EvidenceSourceTier,
    GroundedFact,
)
from investing_monitor.domain.models import OfficialEvent


RULE_BASED_NOTE = "AI 해석 꺼짐 · 방향 판단 없이 원문 사실만 전달"

# Ordered: the first matching type wins, so "Reports Q3 Results and Raises
# Guidance" is earnings, and a supply "agreement" outranks "partnership".
_EVENT_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (EvidenceEventType.EARNINGS.value, (
        "financial results", "quarter results", "quarterly results",
        "earnings", "fiscal year results", "full year results",
    )),
    (EvidenceEventType.GUIDANCE.value, ("guidance", "outlook")),
    (EvidenceEventType.ACQUISITION.value, ("acquire", "acquisition", "merger")),
    (EvidenceEventType.MANAGEMENT.value, (
        "appoints", "appointment", "names new", "chief executive", "chief financial",
        " ceo", " cfo", "board of directors", "resigns", "retire",
    )),
    (EvidenceEventType.FINANCING.value, (
        "convertible", "senior notes", "offering", "pricing of", "credit facility",
        "financing", "warrant",
    )),
    (EvidenceEventType.MAJOR_CONTRACT.value, (
        "agreement", "contract", "order", "selects", "deploy", "supply",
        "power purchase", "megawatt", " mw ", "gigawatt", " gw ",
    )),
    (EvidenceEventType.CAPACITY.value, (
        "capacity", "factory", "manufacturing", "expansion", "facility",
    )),
    (EvidenceEventType.PARTNERSHIP.value, (
        "partnership", "partners with", "collaborat", "alliance",
        "strategic investment", "joint venture",
    )),
    (EvidenceEventType.REGULATORY.value, ("regulator", "antitrust", "investigation", "subpoena")),
    # Product news is briefing-grade: it reaches the close brief, not an
    # immediate alert (evidence_disposition keeps PRODUCT out of IMMEDIATE).
    (EvidenceEventType.PRODUCT.value, ("introduces", "launches", "unveils", "new product", "native power")),
)

_SEC_ITEM_EVENTS = {
    "2.02": EvidenceEventType.EARNINGS.value,
    "1.01": EvidenceEventType.MAJOR_CONTRACT.value,
    "2.01": EvidenceEventType.ACQUISITION.value,
    "5.02": EvidenceEventType.MANAGEMENT.value,
    "2.03": EvidenceEventType.FINANCING.value,
    "3.02": EvidenceEventType.FINANCING.value,
}

_MONTHS = {
    name: index
    for index, name in enumerate(
        ("january", "february", "march", "april", "may", "june", "july",
         "august", "september", "october", "november", "december"),
        start=1,
    )
}
_DATE_PATTERN = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),\s*(20\d{2})\b",
    re.IGNORECASE,
)
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


class RuleBasedEvidenceAnalyzer:
    """Deterministic, network-free stand-in for the Claude analyzer.

    Official sources keep alerting without any model: SEC filings that
    already passed the materiality pre-filter, and IR press releases whose
    headline names a business event (results, contracts, deals, management,
    capacity). Third-party news cannot be judged for relevance without a
    model, so it is recorded but never alerts. Facts are verbatim source
    sentences, and the message says no interpretation was applied.
    """

    def analyze(
        self,
        candidates: Sequence[EvidenceCandidate],
        profile: EvidenceProfile,
    ) -> EvidenceAnalysisBatch:
        analyses = {
            candidate.candidate_id: self._analyze(candidate, profile)
            for candidate in candidates
        }
        return EvidenceAnalysisBatch(analyses, {})

    def _analyze(
        self,
        candidate: EvidenceCandidate,
        profile: EvidenceProfile,
    ) -> EvidenceAnalysis:
        if candidate.kind is EvidenceKind.SEC:
            return self._official(candidate, profile, _sec_event_type(candidate))
        if candidate.kind is EvidenceKind.IR:
            if candidate.metadata.get("calendar_only"):
                return self._calendar(candidate)
            event_type = _headline_event_type(candidate.headline)
            if event_type is None:
                return EvidenceAnalysis(candidate_id=candidate.candidate_id, relevant=False, ai_assisted=False)
            return self._official(candidate, profile, event_type)
        return EvidenceAnalysis(candidate_id=candidate.candidate_id, relevant=False, ai_assisted=False)

    def _official(
        self,
        candidate: EvidenceCandidate,
        profile: EvidenceProfile,
        event_type: str,
    ) -> EvidenceAnalysis:
        form = str(candidate.metadata.get("form") or "")
        prefix = f"{form} 공시" if candidate.kind is EvidenceKind.SEC and form else "회사 발표"
        return EvidenceAnalysis(
            candidate_id=candidate.candidate_id,
            relevant=True,
            headline_ko=f"{prefix}: {candidate.headline.strip()}",
            summary_ko="원문 제목과 본문 문장을 그대로 전달합니다. 요약·번역은 원문 링크에서 확인하세요.",
            facts=_grounded_facts(candidate, profile),
            thesis_impact="neutral",
            impact_reason_ko=RULE_BASED_NOTE,
            confidence="high",
            event_type=event_type,
            company_directness=True,
            new_fact=True,
            materiality=EvidenceMateriality.HIGH.value,
            source_tier=EvidenceSourceTier.OFFICIAL.value,
            alert_worthy=True,
            ai_assisted=False,
        )

    def _calendar(self, candidate: EvidenceCandidate) -> EvidenceAnalysis:
        text = f"{candidate.headline}. {candidate.source_text}"
        events = []
        for match in _DATE_PATTERN.finditer(text):
            month, day, year = match.groups()
            try:
                event_date = date(int(year), _MONTHS[month.casefold()], int(day))
            except ValueError:
                continue
            events.append(
                OfficialEvent(
                    event_date=event_date,
                    title_ko=candidate.headline.strip(),
                    source_url=candidate.source_url,
                    source_text=match.group(0),
                )
            )
            break
        return EvidenceAnalysis(
            candidate_id=candidate.candidate_id,
            relevant=bool(events),
            headline_ko=candidate.headline.strip(),
            official_events=tuple(events),
            source_tier=EvidenceSourceTier.OFFICIAL.value,
            ai_assisted=False,
        )


def _headline_event_type(headline: str) -> str | None:
    text = f" {headline.casefold()} "
    for event_type, patterns in _EVENT_PATTERNS:
        if any(pattern in text for pattern in patterns):
            return event_type
    return None


def _sec_event_type(candidate: EvidenceCandidate) -> str:
    form = str(candidate.metadata.get("form") or "").upper()
    if form.startswith(("10-Q", "10-K", "20-F")):
        return EvidenceEventType.EARNINGS.value
    items = str(candidate.metadata.get("items") or "")
    for item, event_type in _SEC_ITEM_EVENTS.items():
        if item in items:
            return event_type
    return _headline_event_type(candidate.headline) or EvidenceEventType.OTHER.value


def _grounded_facts(
    candidate: EvidenceCandidate,
    profile: EvidenceProfile,
    limit: int = 3,
) -> tuple[GroundedFact, ...]:
    """Pick verbatim sentences carrying numbers or thesis keywords."""
    keywords = tuple(
        word.casefold()
        for word in (*profile.priority_keywords, *profile.core_kpis, *profile.risk_keywords)
        if word.strip()
    )
    sentences = [
        " ".join(sentence.split())
        for sentence in _SENTENCE_SPLIT.split(candidate.source_text or "")
    ]
    scored = []
    for index, sentence in enumerate(sentences):
        if not 40 <= len(sentence) <= 320:
            continue
        lowered = sentence.casefold()
        score = sum(keyword in lowered for keyword in keywords) * 2
        score += len(re.findall(r"\$\s?\d|\d+(?:\.\d+)?\s?%|\d+(?:\.\d+)?\s?(?:mw|gw|million|billion)", lowered))
        if score:
            scored.append((score, index, sentence))
    picked = sorted(sorted(scored, key=lambda item: (-item[0], item[1]))[:limit], key=lambda item: item[1])
    facts = tuple(GroundedFact(source_text=sentence, fact_ko=sentence) for _, _, sentence in picked)
    if facts:
        return facts
    headline = candidate.headline.strip()
    return (GroundedFact(source_text=headline, fact_ko=headline),)
