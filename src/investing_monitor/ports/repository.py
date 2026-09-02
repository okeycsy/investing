from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol, Sequence

from investing_monitor.domain.evidence import (
    CandidateDecision,
    EvidenceAnalysis,
    EvidenceCandidate,
)
from investing_monitor.domain.models import (
    Catalyst,
    MarketFrame,
    PriceBandSignal,
    PriceBandState,
)


@dataclass(frozen=True)
class PendingDelivery:
    outbox_id: int
    event_key: str
    payload: dict
    attempts: int
    status: str = "pending"


@dataclass(frozen=True)
class AlertRecord:
    event_key: str
    ticker: str
    alert_type: str
    created_at: datetime
    payload: dict


class MonitorRepository(Protocol):
    def load_price_band_state(self, ticker: str) -> PriceBandState | None: ...

    def record_price_signal(
        self,
        signal: PriceBandSignal,
        state: PriceBandState,
        payload: dict,
    ) -> bool: ...

    def latest_market_observation_at(self, ticker: str) -> datetime | None: ...

    def record_market_cycle(
        self,
        ticker: str,
        state: PriceBandState,
        frames: Sequence[MarketFrame],
        alerts: Sequence[AlertRecord],
        *,
        enqueue: bool = True,
    ) -> tuple[str, ...]: ...

    def record_evidence_decisions(
        self,
        decisions: Sequence[CandidateDecision],
        cluster_keys: dict[str, str],
        seen_at: datetime,
    ) -> tuple[str, ...]: ...

    def has_source_baseline(self, source_key: str) -> bool: ...

    def record_evidence_baseline(
        self,
        source_key: str,
        decisions: Sequence[CandidateDecision],
        seen_at: datetime,
    ) -> int: ...

    def pending_evidence_candidates(
        self,
        now: datetime,
        limit: int = 20,
    ) -> list[EvidenceCandidate]: ...

    def update_evidence_source_text(
        self,
        candidate_id: str,
        source_text: str,
        metadata: Mapping[str, object] | None = None,
    ) -> None: ...

    def mark_evidence_filtered(
        self,
        candidate_id: str,
        filtered_at: datetime,
        reason: str,
    ) -> None: ...

    def evidence_cluster_key(self, candidate_id: str) -> str: ...

    def record_evidence_analysis(
        self,
        candidate_id: str,
        analysis: EvidenceAnalysis,
        analyzed_at: datetime,
        alert: AlertRecord | None = None,
        *,
        enqueue: bool = True,
    ) -> bool: ...

    def suppress_pending_deliveries(
        self,
        suppressed_at: datetime,
        reason: str,
    ) -> int: ...

    def recent_catalysts(
        self,
        ticker: str,
        since: datetime,
        limit: int = 2,
    ) -> list[Catalyst]: ...

    def mark_evidence_analyzed(
        self,
        candidate_id: str,
        analysis: EvidenceAnalysis,
        analyzed_at: datetime,
    ) -> None: ...

    def mark_evidence_failed(
        self,
        candidate_id: str,
        attempted_at: datetime,
        next_attempt_at: datetime,
        error: str,
    ) -> None: ...

    def pending_deliveries(self, now: datetime, limit: int = 20) -> list[PendingDelivery]: ...

    def mark_sending(self, outbox_id: int, attempted_at: datetime) -> None: ...

    def mark_delivered(self, outbox_id: int, delivered_at: datetime, receipt: str = "") -> None: ...

    def mark_failed(self, outbox_id: int, next_attempt_at: datetime, error: str) -> None: ...

    def mark_delivery_unknown(self, outbox_id: int, attempted_at: datetime, error: str) -> None: ...
