from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit


class EvidenceKind(str, Enum):
    IR = "ir"
    NEWS = "news"
    SEC = "sec"
    INSIDER = "insider"


class EvidenceStatus(str, Enum):
    PENDING = "pending"
    FILTERED = "filtered"
    QUARANTINED = "quarantined"
    BASELINE = "baseline"
    ANALYZED = "analyzed"
    FAILED = "failed"


@dataclass(frozen=True)
class EvidenceProfile:
    ticker: str
    company_name: str
    cik: str
    aliases: tuple[str, ...]
    news_terms: tuple[str, ...] = ()
    priority_keywords: tuple[str, ...] = ()
    risk_keywords: tuple[str, ...] = ()
    core_kpis: tuple[str, ...] = ()
    profile_context: str = ""
    ir_news_url: str = ""
    sec_contact: str = ""

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper().lstrip("$")
        if not ticker:
            raise ValueError("evidence ticker is required")
        if not self.company_name.strip():
            raise ValueError("company_name is required")
        normalized_cik = "".join(character for character in self.cik if character.isdigit())
        normalized_aliases = tuple(
            dict.fromkeys(
                alias.strip()
                for alias in (self.company_name, *self.aliases)
                if alias.strip()
            )
        )
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "company_name", self.company_name.strip())
        object.__setattr__(self, "cik", normalized_cik.zfill(10) if normalized_cik else "")
        object.__setattr__(self, "aliases", normalized_aliases)


@dataclass(frozen=True)
class RawEvidenceCandidate:
    ticker: str
    kind: EvidenceKind
    headline: str = ""
    source_name: str = ""
    source_url: str = ""
    published_at: datetime | None = None
    source_text: str = ""
    external_id: str = ""
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceCandidate:
    candidate_id: str
    ticker: str
    kind: EvidenceKind
    headline: str
    source_name: str
    source_url: str
    published_at: datetime
    source_text: str = ""
    external_id: str = ""
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id is required")
        if not self.headline or not self.source_name:
            raise ValueError("headline and source_name are required")
        if not self.source_url.startswith(("https://", "http://")):
            raise ValueError("traceable source_url is required")
        if self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")


@dataclass(frozen=True)
class CandidateDecision:
    status: EvidenceStatus
    reason: str
    candidate: EvidenceCandidate | None = None
    raw: RawEvidenceCandidate | None = None


@dataclass(frozen=True)
class EvidenceCluster:
    cluster_key: str
    candidates: tuple[EvidenceCandidate, ...]
    representative: EvidenceCandidate


@dataclass(frozen=True)
class GroundedFact:
    source_text: str
    fact_ko: str


@dataclass(frozen=True)
class EvidenceAnalysis:
    candidate_id: str
    relevant: bool
    headline_ko: str = ""
    summary_ko: str = ""
    facts: tuple[GroundedFact, ...] = ()
    interpretation_ko: str = ""
    thesis_impact: str = "neutral"
    impact_reason_ko: str = ""
    confidence: str = "medium"


@dataclass(frozen=True)
class EvidenceDocument:
    source_text: str
    metadata: Mapping[str, object] = field(default_factory=dict)


def candidate_identity(raw: RawEvidenceCandidate) -> str:
    parts = urlsplit(raw.source_url.strip())
    canonical_url = urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path, "", "")
    )
    stable_source_key = raw.external_id.strip() or canonical_url
    if not stable_source_key:
        stable_source_key = " ".join(raw.headline.lower().split())
    value = "|".join(
        (
            raw.ticker.strip().upper(),
            raw.kind.value,
            stable_source_key,
        )
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def extract_filing_items(value: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(re.findall(r"\b(?:item\s+)?(\d\.\d{2})\b", value, re.I))
    )
