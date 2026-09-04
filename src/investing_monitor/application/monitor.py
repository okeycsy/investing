from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketCycle,
    MarketFrame,
    MarketSnapshot,
    PriceBandState,
    VolumeSignal,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    PriceBandPolicy,
    RelativeAssessment,
    VolumeAssessment,
    assess_intraday_volume,
    assess_relative_performance,
)
from investing_monitor.ports.providers import (
    DeliveryOutcomeUnknown,
    DeliveryRejected,
    NotificationPort,
)
from investing_monitor.ports.repository import AlertRecord, MonitorRepository
from investing_monitor.presentation.slack_messages import (
    build_price_band_message,
    build_volume_message,
)


DELAYED_DETECTION_SECONDS = 10 * 60


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
        if volume_assessment.is_exploded:
            next_state = replace(next_state, volume_alerted=True)
        inserted = self.repository.record_price_signal(
            signal,
            next_state,
            payload,
        )
        return signal.event_key if inserted else None


@dataclass(frozen=True)
class MarketCycleReport:
    ticker: str
    trading_date: str
    observed_frames: int
    replayed_frames: int
    inserted_event_keys: tuple[str, ...]
    delayed_event_keys: tuple[str, ...]
    max_detection_delay_seconds: int
    messages: tuple[dict, ...]
    latest_context: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "trading_date": self.trading_date,
            "observed_frames": self.observed_frames,
            "replayed_frames": self.replayed_frames,
            "inserted_event_keys": list(self.inserted_event_keys),
            "delayed_event_keys": list(self.delayed_event_keys),
            "max_detection_delay_seconds": self.max_detection_delay_seconds,
            "messages": list(self.messages),
            "latest_context": dict(self.latest_context),
        }


class MarketCycleService:
    def __init__(
        self,
        repository: MonitorRepository,
        *,
        price_policy: PriceBandPolicy | None = None,
        enqueue_alerts: bool = True,
    ) -> None:
        self.repository = repository
        self.price_policy = price_policy or PriceBandPolicy()
        self.enqueue_alerts = enqueue_alerts

    def process(
        self,
        cycle: MarketCycle,
        catalysts: Sequence[Catalyst] = (),
        *,
        detected_at: datetime | None = None,
    ) -> MarketCycleReport:
        if detected_at is not None and detected_at.tzinfo is None:
            raise ValueError("detected_at must be timezone-aware")
        existing = self.repository.load_price_band_state(cycle.ticker)
        state = self._state_for_cycle(cycle, existing)
        if not cycle.frames:
            return MarketCycleReport(
                ticker=cycle.ticker,
                trading_date=cycle.trading_date.isoformat(),
                observed_frames=0,
                replayed_frames=0,
                inserted_event_keys=(),
                delayed_event_keys=(),
                max_detection_delay_seconds=0,
                messages=(),
                latest_context={},
            )

        selected_frames = self._select_replay_extremes(cycle.frames)
        signals = []
        for frame in selected_frames:
            signal, state = self.price_policy.evaluate(frame.snapshot, state)
            if signal is not None:
                signals.append((signal, frame))

        volume_assessment = assess_intraday_volume(cycle.volume)
        consume_volume = volume_assessment.is_exploded and not state.volume_alerted
        alerts: list[AlertRecord] = []
        payloads: dict[str, dict] = {}
        detection_delays: dict[str, int] = {}
        for signal, frame in signals:
            detection_delay = self._detection_delay(
                signal.observed_at,
                detected_at,
            )
            payload = build_price_band_message(
                signal,
                assess_relative_performance(frame.snapshot),
                cycle.volume,
                volume_assessment,
                catalysts,
                detection_delay_seconds=detection_delay,
            )
            alerts.append(
                AlertRecord(
                    event_key=signal.event_key,
                    ticker=signal.ticker,
                    alert_type="price_band",
                    created_at=signal.observed_at,
                    payload=payload,
                )
            )
            payloads[signal.event_key] = payload
            detection_delays[signal.event_key] = detection_delay

        latest = cycle.frames[-1].snapshot
        latest_relative = assess_relative_performance(latest)
        if consume_volume and not signals and cycle.volume is not None:
            signal = VolumeSignal(
                event_key=(
                    f"{cycle.ticker.upper()}:{cycle.trading_date.isoformat()}:volume-spike"
                ),
                ticker=cycle.ticker.upper(),
                trading_date=cycle.trading_date,
                observed_at=latest.observed_at,
            )
            detection_delay = self._detection_delay(
                signal.observed_at,
                detected_at,
            )
            payload = build_volume_message(
                signal,
                latest,
                latest_relative,
                cycle.volume,
                volume_assessment,
                detection_delay_seconds=detection_delay,
            )
            alerts.append(
                AlertRecord(
                    event_key=signal.event_key,
                    ticker=signal.ticker,
                    alert_type="volume_spike",
                    created_at=signal.observed_at,
                    payload=payload,
                )
            )
            payloads[signal.event_key] = payload
            detection_delays[signal.event_key] = detection_delay

        if consume_volume:
            state = replace(state, volume_alerted=True)

        inserted = self.repository.record_market_cycle(
            cycle.ticker,
            state,
            cycle.frames,
            cycle.volume,
            alerts,
            enqueue=self.enqueue_alerts,
        )
        delayed = tuple(
            key
            for key in inserted
            if detection_delays.get(key, 0) > DELAYED_DETECTION_SECONDS
        )
        return MarketCycleReport(
            ticker=cycle.ticker,
            trading_date=cycle.trading_date.isoformat(),
            observed_frames=len(cycle.frames),
            replayed_frames=cycle.replayed_frames,
            inserted_event_keys=inserted,
            delayed_event_keys=delayed,
            max_detection_delay_seconds=max(
                (detection_delays[key] for key in inserted),
                default=0,
            ),
            messages=tuple(payloads[key] for key in inserted),
            latest_context=self._latest_context(
                latest,
                latest_relative,
                cycle.volume,
                volume_assessment,
            ),
        )

    @staticmethod
    def _detection_delay(
        observed_at: datetime,
        detected_at: datetime | None,
    ) -> int:
        if detected_at is None:
            return 0
        return max(
            0,
            int(
                (
                    detected_at.astimezone(timezone.utc)
                    - observed_at.astimezone(timezone.utc)
                ).total_seconds()
            ),
        )

    @staticmethod
    def _state_for_cycle(
        cycle: MarketCycle,
        state: PriceBandState | None,
    ) -> PriceBandState:
        if state is None or state.trading_date != cycle.trading_date:
            return PriceBandState(trading_date=cycle.trading_date)
        return state

    @staticmethod
    def _select_replay_extremes(frames: Sequence[MarketFrame]) -> tuple[MarketFrame, ...]:
        candidates: list[MarketFrame] = []
        positive = [frame for frame in frames if frame.snapshot.direction is Direction.UP]
        negative = [frame for frame in frames if frame.snapshot.direction is Direction.DOWN]
        if positive:
            candidates.append(max(positive, key=lambda frame: frame.snapshot.change_pct))
        if negative:
            candidates.append(min(negative, key=lambda frame: frame.snapshot.change_pct))
        return tuple(sorted(candidates, key=lambda frame: frame.snapshot.observed_at))

    @staticmethod
    def _latest_context(
        snapshot: MarketSnapshot,
        relative: RelativeAssessment,
        volume: VolumeSnapshot | None,
        volume_assessment: VolumeAssessment,
    ) -> dict[str, object]:
        context: dict[str, object] = {
            "direction": snapshot.direction.value,
            "benchmark": {
                "symbol": relative.benchmark_symbol,
                "outcome": relative.benchmark.value,
            },
            "peers": {
                "symbols": list(relative.peer_symbols),
                "outcome": relative.peers.value,
            },
        }
        if volume is not None and volume_assessment.is_ready:
            context["volume"] = {
                "status": "exploded" if volume_assessment.is_exploded else "normal",
                "ratio": round(volume_assessment.ratio or 0.0, 2),
                "observed": volume.observed_volume,
                "expected": volume.expected_volume,
                "baseline_sessions": volume.baseline_sessions,
            }
        else:
            context["volume"] = {"status": "unavailable"}
        return context


class OutboxDeliveryService:
    def __init__(
        self,
        repository: MonitorRepository,
        notifier: NotificationPort,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.repository = repository
        self.notifier = notifier
        self.checkpoint = checkpoint or (lambda _reason: None)

    async def deliver_pending(self, *, limit: int = 20) -> int:
        now = datetime.now(timezone.utc)
        delivered = 0
        for item in self.repository.pending_deliveries(now, limit=limit):
            self.repository.mark_sending(item.outbox_id, now)
            try:
                self.checkpoint(f"sending:{item.event_key}")
            except Exception as exc:
                self.repository.mark_failed(
                    item.outbox_id,
                    now,
                    f"pre-send checkpoint failed: {exc}",
                )
                raise
            try:
                receipt = await self.notifier.send(item.payload)
            except DeliveryOutcomeUnknown as exc:
                self.repository.mark_delivery_unknown(item.outbox_id, now, str(exc))
                self.checkpoint(f"delivery-unknown:{item.event_key}")
                continue
            except DeliveryRejected as exc:
                if not exc.retryable:
                    self.repository.mark_discarded(item.outbox_id, now, str(exc))
                    self.checkpoint(f"delivery-discarded:{item.event_key}")
                    continue
                delay_seconds = exc.retry_after_seconds or min(
                    30 * (2 ** item.attempts),
                    15 * 60,
                )
                self.repository.mark_failed(
                    item.outbox_id,
                    now + timedelta(seconds=delay_seconds),
                    str(exc),
                )
                self.checkpoint(f"delivery-failed:{item.event_key}")
                continue
            except Exception as exc:
                delay_seconds = min(30 * (2 ** item.attempts), 15 * 60)
                self.repository.mark_failed(
                    item.outbox_id,
                    now + timedelta(seconds=delay_seconds),
                    str(exc),
                )
                self.checkpoint(f"delivery-failed:{item.event_key}")
                continue
            self.repository.mark_delivered(item.outbox_id, datetime.now(timezone.utc), receipt)
            self.checkpoint(f"delivered:{item.event_key}")
            delivered += 1
        return delivered
