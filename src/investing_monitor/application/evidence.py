from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from collections.abc import Callable
from typing import Protocol, Sequence
from urllib.parse import urlsplit, urlunsplit

from investing_monitor.domain.evidence import (
    CandidateDecision,
    EvidenceAnalysis,
    EvidenceCandidate,
    EvidenceCluster,
    EvidenceKind,
    EvidenceProfile,
    EvidenceStatus,
    EvidenceDocument,
    RawEvidenceCandidate,
    candidate_identity,
)
from investing_monitor.application.insider import (
    assess_insider_materiality,
    build_insider_analysis,
)
from investing_monitor.application.filings import assess_filing_materiality
from investing_monitor.ports.repository import AlertRecord, MonitorRepository


LOW_VALUE_PATTERNS = (
    "price target",
    "target price",
    "fair value",
    "overvalued",
    "undervalued",
    "better buy",
    "stock to buy",
    "stocks to buy",
    "top stock",
    "top 3",
    "stocks to watch",
    "stocks with",
    "overlooked stocks",
    "isn't just about",
    "is not just about",
    "buy, sell, or hold",
    "buy sell or hold",
    "may be fairly valued",
    "market outlook",
    "featuring profiles",
    "industry to reach",
    "declares quarterly dividend",
    "participate in upcoming investor conference",
    "participate in upcoming conference",
    "announces date of",
    "versus",
    " vs ",
    "investors who lost money",
    "shareholder alert",
    "securities fraud investigation",
    "class action",
    "목표주가",
    "적정가치",
    "고평가",
    "저평가",
    "비교",
    "집단소송",
)

CALENDAR_ONLY_PATTERNS = (
    "announces date of",
    "participate in upcoming investor conference",
    "participate in upcoming investor conferences",
    "to host investor conference",
)

TOKEN_ALIASES = {
    "acquisition": "acquire",
    "acquisitions": "acquire",
    "acquired": "acquire",
    "acquires": "acquire",
    "acquiring": "acquire",
    "collaborates": "partner",
    "collaboration": "partner",
    "partnership": "partner",
    "partners": "partner",
    "announced": "announce",
    "announces": "announce",
    "announcement": "announce",
    "results": "earnings",
    "result": "earnings",
    "deal": "acquire",
}

STOP_WORDS = {
    "a",
    "an",
    "and",
    "co",
    "company",
    "corp",
    "corporation",
    "for",
    "from",
    "holdings",
    "in",
    "inc",
    "of",
    "on",
    "the",
    "to",
    "with",
    "ai",
    "announce",
    "center",
    "data",
    "vertiv",
    "vrt",
}


def screen_candidate(
    raw: RawEvidenceCandidate,
    profile: EvidenceProfile,
) -> CandidateDecision:
    missing = []
    if not raw.headline.strip():
        missing.append("headline")
    if not raw.source_name.strip():
        missing.append("source_name")
    if not raw.source_url.startswith(("https://", "http://")):
        missing.append("source_url")
    if raw.published_at is None or raw.published_at.tzinfo is None:
        missing.append("published_at")
    if missing:
        return CandidateDecision(
            status=EvidenceStatus.QUARANTINED,
            reason=f"missing required fields: {','.join(missing)}",
            raw=raw,
        )

    candidate = EvidenceCandidate(
        candidate_id=candidate_identity(raw),
        ticker=profile.ticker,
        kind=raw.kind,
        headline=" ".join(raw.headline.split()),
        source_name=" ".join(raw.source_name.split()),
        source_url=_canonical_url(raw.source_url),
        published_at=raw.published_at.astimezone(timezone.utc),
        source_text=" ".join(raw.source_text.split()),
        external_id=raw.external_id.strip(),
        metadata=dict(raw.metadata),
    )
    title = f" {candidate.headline.lower()} "
    calendar_only = candidate.kind is EvidenceKind.IR and any(
        pattern in title for pattern in CALENDAR_ONLY_PATTERNS
    )
    if calendar_only:
        candidate = replace(
            candidate,
            metadata={**candidate.metadata, "calendar_only": True},
        )
    if candidate.kind in {EvidenceKind.NEWS, EvidenceKind.IR} and any(
        pattern in title for pattern in LOW_VALUE_PATTERNS
    ) and not calendar_only:
        return CandidateDecision(
            status=EvidenceStatus.FILTERED,
            reason="deterministic low-value title rule",
            candidate=candidate,
        )
    if candidate.kind is EvidenceKind.INSIDER:
        assessment = assess_insider_materiality(candidate)
        if assessment is not None and not assessment.material:
            return CandidateDecision(
                status=EvidenceStatus.FILTERED,
                reason=assessment.reason,
                candidate=candidate,
            )
    if candidate.kind is EvidenceKind.SEC:
        assessment = assess_filing_materiality(candidate, profile)
        if assessment is not None and not assessment.material:
            return CandidateDecision(
                status=EvidenceStatus.FILTERED,
                reason=assessment.reason,
                candidate=candidate,
            )
    return CandidateDecision(
        status=EvidenceStatus.PENDING,
        reason="ready for evidence analysis",
        candidate=candidate,
    )


def cluster_candidates(
    candidates: tuple[EvidenceCandidate, ...] | list[EvidenceCandidate],
    *,
    window: timedelta = timedelta(minutes=15),
    minimum_similarity: float = 0.4,
    extended_window: timedelta = timedelta(hours=24),
    extended_similarity: float = 0.5,
) -> tuple[EvidenceCluster, ...]:
    groups: list[list[EvidenceCandidate]] = []
    for candidate in sorted(candidates, key=lambda item: item.published_at):
        tokens = _event_tokens(candidate.headline)
        match: list[EvidenceCandidate] | None = None
        for group in groups:
            comparisons = [
                (
                    abs(candidate.published_at - member.published_at),
                    _overlap(tokens, _event_tokens(member.headline)),
                )
                for member in group
            ]
            if any(
                (distance <= window and similarity >= minimum_similarity)
                or (
                    distance <= extended_window
                    and similarity >= extended_similarity
                )
                for distance, similarity in comparisons
            ):
                match = group
                break
        if match is None:
            groups.append([candidate])
        else:
            match.append(candidate)

    clusters = []
    for group in groups:
        representative = _representative(group)
        identity = min(candidate.candidate_id for candidate in group)
        cluster_hash = hashlib.sha256(identity.encode("ascii")).hexdigest()[:20]
        clusters.append(
            EvidenceCluster(
                cluster_key=f"{representative.ticker}:evidence:{cluster_hash}",
                candidates=tuple(group),
                representative=representative,
            )
        )
    return tuple(clusters)


def _representative(candidates: list[EvidenceCandidate]) -> EvidenceCandidate:
    source_rank = {
        EvidenceKind.IR: 4,
        EvidenceKind.SEC: 3,
        EvidenceKind.INSIDER: 2,
        EvidenceKind.NEWS: 1,
    }
    return max(
        candidates,
        key=lambda item: (
            bool(item.source_text.strip()),
            source_rank[item.kind],
            _source_authority(item.source_name),
            len(item.source_text),
            -len(item.source_url),
        ),
    )


def _event_tokens(value: str) -> set[str]:
    camel_split = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)
    tokens = re.findall(r"[a-z0-9가-힣]+", camel_split.lower())
    return {
        normalized
        for token in tokens
        if len(token) > 1
        if (normalized := TOKEN_ALIASES.get(token, token)) not in STOP_WORDS
    }


def _overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def same_evidence_event(
    left_candidate: EvidenceCandidate,
    left_analysis: EvidenceAnalysis,
    right_candidate: EvidenceCandidate,
    right_analysis: EvidenceAnalysis,
    *,
    window: timedelta = timedelta(hours=24),
) -> bool:
    if abs(left_candidate.published_at - right_candidate.published_at) > window:
        return False
    similarities = []
    for left in _event_signatures(left_candidate, left_analysis):
        for right in _event_signatures(right_candidate, right_analysis):
            shared = left & right
            if len(shared) >= 2:
                similarities.append(_overlap(left, right))
    return bool(similarities) and max(similarities) >= 0.5


def _event_signatures(
    candidate: EvidenceCandidate,
    analysis: EvidenceAnalysis,
) -> tuple[set[str], ...]:
    return tuple(
        tokens
        for tokens in (
            _event_tokens(candidate.headline),
            _event_tokens(analysis.headline_ko),
        )
        if tokens
    )


def _canonical_url(value: str) -> str:
    parts = urlsplit(value.strip())
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, "", ""))


def _source_authority(value: str) -> int:
    normalized = value.casefold()
    if any(name in normalized for name in ("pr newswire", "business wire")):
        return 3
    if any(name in normalized for name in ("reuters", "associated press")):
        return 2
    if "globenewswire" in normalized:
        return 1
    return 0


class EvidenceAnalyzer(Protocol):
    def analyze(self, candidates, profile): ...


class ArticleTextProvider(Protocol):
    def fetch(self, url: str) -> str: ...


class FilingTextProvider(Protocol):
    def fetch(self, candidate: EvidenceCandidate) -> str | EvidenceDocument: ...


@dataclass(frozen=True)
class EvidenceIngestionReport:
    seen: int
    inserted_pending: int
    filtered: int
    quarantined: int
    enriched: int
    analyzed: int
    relevant: int
    failed: int
    alerts: int

    def as_dict(self) -> dict[str, int]:
        return {
            "seen": self.seen,
            "inserted_pending": self.inserted_pending,
            "filtered": self.filtered,
            "quarantined": self.quarantined,
            "enriched": self.enriched,
            "analyzed": self.analyzed,
            "relevant": self.relevant,
            "failed": self.failed,
            "alerts": self.alerts,
        }


class EvidenceIngestionService:
    def __init__(
        self,
        repository: MonitorRepository,
        profile: EvidenceProfile,
        analyzer: EvidenceAnalyzer,
        *,
        article_text: ArticleTextProvider | None = None,
        filing_text: FilingTextProvider | None = None,
        alert_builder: Callable[[EvidenceCandidate, EvidenceAnalysis], dict]
        | None = None,
        lookback: timedelta = timedelta(hours=24),
        retry_delay: timedelta = timedelta(minutes=5),
        batch_limit: int = 5,
        enqueue_alerts: bool = True,
    ) -> None:
        self.repository = repository
        self.profile = profile
        self.analyzer = analyzer
        self.article_text = article_text
        self.filing_text = filing_text
        self.alert_builder = alert_builder
        self.lookback = lookback
        self.retry_delay = retry_delay
        self.batch_limit = batch_limit
        self.enqueue_alerts = enqueue_alerts

    def ingest(
        self,
        raw_candidates: Sequence[RawEvidenceCandidate],
        now: datetime,
    ) -> EvidenceIngestionReport:
        now = now.astimezone(timezone.utc)
        decisions = [screen_candidate(raw, self.profile) for raw in raw_candidates]
        decisions = [self._apply_lookback(decision, now) for decision in decisions]
        pending = [
            decision.candidate
            for decision in decisions
            if decision.status is EvidenceStatus.PENDING and decision.candidate is not None
        ]
        clusters = cluster_candidates(pending)
        cluster_keys = {
            candidate.candidate_id: cluster.cluster_key
            for cluster in clusters
            for candidate in cluster.candidates
        }
        duplicate_ids = {
            candidate.candidate_id
            for cluster in clusters
            for candidate in cluster.candidates
            if candidate.candidate_id != cluster.representative.candidate_id
        }
        decisions = [
            CandidateDecision(
                status=EvidenceStatus.FILTERED,
                reason=f"cluster duplicate: {cluster_keys[decision.candidate.candidate_id]}",
                candidate=decision.candidate,
            )
            if decision.candidate is not None
            and decision.candidate.candidate_id in duplicate_ids
            else decision
            for decision in decisions
        ]
        inserted = self.repository.record_evidence_decisions(
            decisions,
            cluster_keys,
            now,
        )
        analysis_candidates = self.repository.pending_evidence_candidates(
            now,
            limit=self.batch_limit,
        )
        (
            analysis_candidates,
            enriched,
            enrichment_errors,
            post_enrichment_filters,
        ) = self._enrich(analysis_candidates)
        analyzed = relevant = alerts = 0
        failed = len(enrichment_errors)
        for candidate_id, error in enrichment_errors.items():
            self.repository.mark_evidence_failed(
                candidate_id,
                now,
                now + self.retry_delay,
                error,
            )
        for candidate_id, reason in post_enrichment_filters.items():
            self.repository.mark_evidence_filtered(candidate_id, now, reason)

        structured_insiders = [
            candidate
            for candidate in analysis_candidates
            if candidate.kind is EvidenceKind.INSIDER
            and assess_insider_materiality(candidate) is not None
        ]
        analysis_candidates = [
            candidate
            for candidate in analysis_candidates
            if candidate not in structured_insiders
        ]
        for candidate in structured_insiders:
            analysis = build_insider_analysis(candidate)
            alert = self._build_alert(candidate, analysis)
            inserted_alert = self.repository.record_evidence_analysis(
                candidate.candidate_id,
                analysis,
                now,
                alert,
                enqueue=self.enqueue_alerts,
            )
            analyzed += 1
            relevant += 1
            alerts += int(inserted_alert)
        if analysis_candidates:
            try:
                batch = self.analyzer.analyze(analysis_candidates, self.profile)
            except Exception as exc:
                batch = None
                for candidate in analysis_candidates:
                    self.repository.mark_evidence_failed(
                        candidate.candidate_id,
                        now,
                        now + self.retry_delay,
                        str(exc),
                    )
                    failed += 1
            if batch is not None:
                candidates_by_id = {
                    candidate.candidate_id: candidate
                    for candidate in analysis_candidates
                }
                for candidate_id, analysis in batch.analyses.items():
                    candidate = candidates_by_id[candidate_id]
                    alert = self._build_alert(candidate, analysis)
                    inserted_alert = self.repository.record_evidence_analysis(
                        candidate_id,
                        analysis,
                        now,
                        alert,
                        enqueue=self.enqueue_alerts,
                    )
                    analyzed += 1
                    relevant += int(analysis.relevant)
                    alerts += int(inserted_alert)
                for candidate_id, error in batch.errors.items():
                    self.repository.mark_evidence_failed(
                        candidate_id,
                        now,
                        now + self.retry_delay,
                        error,
                    )
                    failed += 1
        return EvidenceIngestionReport(
            seen=len(raw_candidates),
            inserted_pending=len(inserted),
            filtered=sum(
                decision.status is EvidenceStatus.FILTERED for decision in decisions
            )
            + len(post_enrichment_filters),
            quarantined=sum(
                decision.status is EvidenceStatus.QUARANTINED for decision in decisions
            ),
            enriched=enriched,
            analyzed=analyzed,
            relevant=relevant,
            failed=failed,
            alerts=alerts,
        )

    def _apply_lookback(
        self,
        decision: CandidateDecision,
        now: datetime,
    ) -> CandidateDecision:
        candidate = decision.candidate
        if (
            decision.status is EvidenceStatus.PENDING
            and candidate is not None
            and candidate.published_at < now - self.lookback
        ):
            return CandidateDecision(
                status=EvidenceStatus.FILTERED,
                reason="outside evidence lookback",
                candidate=candidate,
            )
        return decision

    def _enrich(
        self,
        candidates: Sequence[EvidenceCandidate],
    ) -> tuple[
        list[EvidenceCandidate],
        int,
        dict[str, str],
        dict[str, str],
    ]:
        enriched = 0
        results = []
        errors = {}
        filtered = {}
        for candidate in candidates:
            host = urlsplit(candidate.source_url).netloc.lower()
            should_fetch = (
                self.article_text is not None
                and candidate.kind is EvidenceKind.NEWS
                and host.endswith("finance.yahoo.com")
                and candidate.source_text.strip() == candidate.headline.strip()
            )
            if should_fetch:
                try:
                    source_text = self.article_text.fetch(candidate.source_url)
                except Exception:
                    source_text = ""
                if source_text:
                    candidate = replace(candidate, source_text=source_text)
                    self.repository.update_evidence_source_text(
                        candidate.candidate_id,
                        source_text,
                    )
                    enriched += 1
            needs_filing_text = candidate.kind is EvidenceKind.SEC or (
                candidate.kind is EvidenceKind.INSIDER
                and not candidate.metadata.get("transaction_code")
            )
            if needs_filing_text:
                if len(candidate.source_text) < 200:
                    if self.filing_text is None:
                        errors[candidate.candidate_id] = "filing text provider unavailable"
                        continue
                    try:
                        document = self.filing_text.fetch(candidate)
                    except Exception as exc:
                        errors[candidate.candidate_id] = str(exc)
                        continue
                    if isinstance(document, EvidenceDocument):
                        source_text = document.source_text
                        metadata = {**candidate.metadata, **document.metadata}
                    else:
                        source_text = document
                        metadata = candidate.metadata
                    candidate = replace(
                        candidate,
                        source_text=source_text,
                        metadata=metadata,
                    )
                    self.repository.update_evidence_source_text(
                        candidate.candidate_id,
                        source_text,
                        metadata,
                    )
                    enriched += 1
            if candidate.kind is EvidenceKind.INSIDER:
                assessment = assess_insider_materiality(candidate)
                if assessment is not None and not assessment.material:
                    filtered[candidate.candidate_id] = assessment.reason
                    continue
            if candidate.kind is EvidenceKind.SEC:
                assessment = assess_filing_materiality(candidate, self.profile)
                if assessment is not None and not assessment.material:
                    filtered[candidate.candidate_id] = assessment.reason
                    continue
            results.append(candidate)
        return results, enriched, errors, filtered

    def _build_alert(
        self,
        candidate: EvidenceCandidate,
        analysis: EvidenceAnalysis,
    ) -> AlertRecord | None:
        if (
            not analysis.relevant
            or self.alert_builder is None
            or candidate.metadata.get("calendar_only")
        ):
            return None
        for existing in self.repository.recent_analyzed_evidence(
            candidate.ticker,
            candidate.published_at - timedelta(hours=24),
        ):
            if same_evidence_event(
                candidate,
                analysis,
                existing.candidate,
                existing.analysis,
            ):
                self.repository.link_evidence_cluster(
                    candidate.candidate_id,
                    existing.cluster_key,
                )
                return None
        return AlertRecord(
            event_key=self.repository.evidence_cluster_key(candidate.candidate_id),
            ticker=candidate.ticker,
            alert_type={
                EvidenceKind.NEWS: "catalyst",
                EvidenceKind.IR: "catalyst",
                EvidenceKind.SEC: "filing",
                EvidenceKind.INSIDER: "insider",
            }[candidate.kind],
            created_at=candidate.published_at,
            payload=self.alert_builder(candidate, analysis),
        )
