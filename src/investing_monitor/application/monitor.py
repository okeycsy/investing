from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

from investing_monitor.domain.models import Catalyst, MarketSnapshot, VolumeSnapshot
from investing_monitor.domain.policies import (
    PriceBandPolicy,
    assess_intraday_volume,
    assess_relative_performance,
)
from investing_monitor.ports.providers import DeliveryOutcomeUnknown, NotificationPort
from investing_monitor.ports.repository import MonitorRepository
from investing_monitor.presentation.slack_messages import build_price_band_message


class MarketMonitorService:
    def __init__(
        self,
        repository: MonitorRepository,
        *,
        price_policy: PriceBandPolicy | None = None,
    ) -> None:
        self.repository = repository
        self.price_policy = price_policy or PriceBandPolicy()

    def handle_snapshot(
        self,
        snapshot: MarketSnapshot,
        *,
        volume: VolumeSnapshot | None = None,
        catalysts: Sequence[Catalyst] = (),
    ) -> str | None:
        state = self.repository.load_price_band_state(snapshot.ticker)
        signal, next_state = self.price_policy.evaluate(snapshot, state)
        if signal is None:
            return None

        relative = assess_relative_performance(snapshot)
        volume_assessment = assess_intraday_volume(volume)
        payload = build_price_band_message(
            signal,
            relative,
            volume,
            volume_assessment,
            catalysts,
        )
        inserted = self.repository.record_price_signal(
            signal,
            next_state,
            payload,
        )
        return signal.event_key if inserted else None


class OutboxDeliveryService:
    def __init__(
        self,
        repository: MonitorRepository,
        notifier: NotificationPort,
    ) -> None:
        self.repository = repository
        self.notifier = notifier

    async def deliver_pending(self, *, limit: int = 20) -> int:
        now = datetime.now(timezone.utc)
        delivered = 0
        for item in self.repository.pending_deliveries(now, limit=limit):
            self.repository.mark_sending(item.outbox_id, now)
            try:
                receipt = await self.notifier.send(item.payload)
            except DeliveryOutcomeUnknown as exc:
                self.repository.mark_delivery_unknown(item.outbox_id, now, str(exc))
                continue
            except Exception as exc:
                delay_seconds = min(30 * (2 ** item.attempts), 15 * 60)
                self.repository.mark_failed(
                    item.outbox_id,
                    now + timedelta(seconds=delay_seconds),
                    str(exc),
                )
                continue
            self.repository.mark_delivered(item.outbox_id, datetime.now(timezone.utc), receipt)
            delivered += 1
        return delivered
