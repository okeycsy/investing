from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Protocol

from investing_monitor.domain.models import Catalyst, MarketSnapshot


class DeliveryOutcomeUnknown(RuntimeError):
    """The provider may have accepted a notification before the request failed."""


class DeliveryRejected(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class MarketDataPort(Protocol):
    def stream(self) -> AsyncIterator[MarketSnapshot]: ...


class CatalystPort(Protocol):
    async def fetch_since(self, since: datetime) -> Sequence[Catalyst]: ...


class EvidenceAnalysisPort(Protocol):
    async def analyze(self, candidates: Sequence[Catalyst]) -> Sequence[Catalyst]: ...


class NotificationPort(Protocol):
    async def send(self, payload: dict) -> str: ...
