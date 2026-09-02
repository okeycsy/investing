from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import MarketCycle
from investing_monitor.presentation.quality import audit_message


class ReplayCalendar(Protocol):
    def regular_open(self, value: date) -> datetime: ...

    def regular_close(self, value: date) -> datetime: ...


class ReplayMarketProvider(Protocol):
    def fetch_cycle(
        self,
        now: datetime,
        *,
        last_observed_at: datetime | None,
    ) -> MarketCycle: ...


@dataclass(frozen=True)
class ReplayDayReport:
    trading_date: str
    observed_frames: int
    replayed_frames: int
    event_keys: tuple[str, ...]
    repeated_event_keys: tuple[str, ...]
    quality_passed: bool
    quality_violations: tuple[str, ...]
    latest_context: dict[str, object]
    messages: tuple[dict, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "trading_date": self.trading_date,
            "observed_frames": self.observed_frames,
            "replayed_frames": self.replayed_frames,
            "event_keys": list(self.event_keys),
            "repeated_event_keys": list(self.repeated_event_keys),
            "quality_passed": self.quality_passed,
            "quality_violations": list(self.quality_violations),
            "latest_context": self.latest_context,
            "messages": list(self.messages),
        }


class MarketReplayLab:
    def __init__(
        self,
        provider: ReplayMarketProvider,
        calendar: ReplayCalendar,
        service: MarketCycleService,
    ) -> None:
        self.provider = provider
        self.calendar = calendar
        self.service = service

    def replay_day(self, trading_date: date) -> ReplayDayReport:
        replay_at = self.calendar.regular_close(trading_date) - timedelta(seconds=1)
        cursor = self.calendar.regular_open(trading_date) - timedelta(minutes=5)
        cycle = self.provider.fetch_cycle(
            replay_at,
            last_observed_at=cursor,
        )
        first = self.service.process(cycle)
        repeated = self.service.process(cycle)
        violations = []
        for message in first.messages:
            result = audit_message(_alert_type_for(message, first.inserted_event_keys), message)
            violations.extend(result.violations)
        if repeated.inserted_event_keys:
            violations.append("identical replay created duplicate events")
        return ReplayDayReport(
            trading_date=trading_date.isoformat(),
            observed_frames=first.observed_frames,
            replayed_frames=first.replayed_frames,
            event_keys=first.inserted_event_keys,
            repeated_event_keys=repeated.inserted_event_keys,
            quality_passed=not violations,
            quality_violations=tuple(dict.fromkeys(violations)),
            latest_context=dict(first.latest_context),
            messages=first.messages,
        )


def _alert_type_for(message: dict, event_keys: tuple[str, ...]) -> str:
    text = str(message.get("text") or "")
    if "거래량" in text and not any("price-band" in key for key in event_keys):
        return "volume_spike"
    return "price_band"
