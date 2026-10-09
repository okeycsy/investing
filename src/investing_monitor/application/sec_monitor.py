from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from investing_monitor.adapters.sec_filings import (
    ResilientSecFilingsAdapter,
    SecFilingError,
)
from investing_monitor.application.evidence import (
    EvidenceIngestionReport,
    EvidenceIngestionService,
    screen_candidate,
)
from investing_monitor.domain.evidence import EvidenceProfile
from investing_monitor.ports.repository import MonitorRepository


# Filings reach the Yahoo mirror (and a sparsely polled EDGAR) later than the
# news feeds, so SEC evidence gets a wider ingestion window than the default.
SEC_EVIDENCE_LOOKBACK = timedelta(hours=72)


@dataclass(frozen=True)
class SecPollReport:
    provider: str
    recovered: bool
    baseline_created: bool
    baseline_candidates: int
    ingestion: EvidenceIngestionReport | None = None
    primary_error: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "recovered": self.recovered,
            "baseline_created": self.baseline_created,
            "baseline_candidates": self.baseline_candidates,
            "ingestion": self.ingestion.as_dict() if self.ingestion else None,
            "primary_error": self.primary_error,
        }


class SecMonitorService:
    def __init__(
        self,
        repository: MonitorRepository,
        profile: EvidenceProfile,
        adapter: ResilientSecFilingsAdapter,
        ingestion: EvidenceIngestionService,
    ) -> None:
        self.repository = repository
        self.profile = profile
        self.adapter = adapter
        self.ingestion = ingestion

    def poll(self, now: datetime) -> SecPollReport:
        result = self.adapter.fetch(self.profile, now=now)
        source_key = f"sec:{self.profile.ticker}:{self.profile.cik}"
        if not self.repository.has_source_baseline(source_key):
            if not result.candidates:
                raise SecFilingError("SEC source returned no candidates for initial baseline")
            decisions = [
                screen_candidate(candidate, self.profile)
                for candidate in result.candidates
            ]
            count = self.repository.record_evidence_baseline(
                source_key,
                decisions,
                now,
            )
            return SecPollReport(
                provider=result.provider,
                recovered=result.recovered,
                baseline_created=True,
                baseline_candidates=count,
                primary_error=result.primary_error,
            )
        report = self.ingestion.ingest(
            result.candidates,
            now,
            lookback=SEC_EVIDENCE_LOOKBACK,
        )
        return SecPollReport(
            provider=result.provider,
            recovered=result.recovered,
            baseline_created=False,
            baseline_candidates=0,
            ingestion=report,
            primary_error=result.primary_error,
        )
