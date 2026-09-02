from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Protocol

from investing_monitor.domain.models import Catalyst, MarketSnapshot


class DeliveryOutcomeUnknown(RuntimeError):
    """The provider may have accepted a notification before the request failed."""


class MarketDataPort(Protocol):
    def stream(self) -> AsyncIterator[MarketSnapshot]: ...


class CatalystPort(Protocol):
    async def fetch_since(self, since: datetime) -> Sequence[Catalyst]: ...


class EvidenceAnalysisPort(Protocol):
    async def analyze(self, candidates: Sequence[Catalyst]) -> Sequence[Catalyst]: ...


class NotificationPort(Protocol):
    async def send(self, payload: dict) -> str: ...
