from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from investing_monitor.domain.models import PriceBandSignal, PriceBandState


@dataclass(frozen=True)
class PendingDelivery:
    outbox_id: int
    event_key: str
    payload: dict
    attempts: int
    status: str = "pending"


class MonitorRepository(Protocol):
    def load_price_band_state(self, ticker: str) -> PriceBandState | None: ...

    def record_price_signal(
        self,
        signal: PriceBandSignal,
        state: PriceBandState,
        payload: dict,
    ) -> bool: ...

    def pending_deliveries(self, now: datetime, limit: int = 20) -> list[PendingDelivery]: ...

    def mark_sending(self, outbox_id: int, attempted_at: datetime) -> None: ...

    def mark_delivered(self, outbox_id: int, delivered_at: datetime, receipt: str = "") -> None: ...

    def mark_failed(self, outbox_id: int, next_attempt_at: datetime, error: str) -> None: ...

    def mark_delivery_unknown(self, outbox_id: int, attempted_at: datetime, error: str) -> None: ...
