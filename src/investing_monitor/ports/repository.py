from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence

from investing_monitor.domain.models import MarketFrame, PriceBandSignal, PriceBandState


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
    ) -> tuple[str, ...]: ...

    def pending_deliveries(self, now: datetime, limit: int = 20) -> list[PendingDelivery]: ...

    def mark_sending(self, outbox_id: int, attempted_at: datetime) -> None: ...

    def mark_delivered(self, outbox_id: int, delivered_at: datetime, receipt: str = "") -> None: ...

    def mark_failed(self, outbox_id: int, next_attempt_at: datetime, error: str) -> None: ...

    def mark_delivery_unknown(self, outbox_id: int, attempted_at: datetime, error: str) -> None: ...
