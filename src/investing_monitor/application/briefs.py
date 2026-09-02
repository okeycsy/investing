from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from investing_monitor.domain.policies import (
    assess_intraday_volume,
    assess_relative_performance,
)
from investing_monitor.ports.repository import AlertRecord, MonitorRepository
from investing_monitor.presentation.close_messages import build_close_message


@dataclass(frozen=True)
class CloseBriefReport:
    ticker: str
    trading_date: str
    event_key: str
    inserted: bool
    catalyst_count: int
    payload: dict

    def as_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "trading_date": self.trading_date,
            "event_key": self.event_key,
            "inserted": self.inserted,
            "catalyst_count": self.catalyst_count,
            "message": self.payload,
        }


class CloseBriefService:
    def __init__(
        self,
        repository: MonitorRepository,
        *,
        enqueue_alerts: bool = True,
    ) -> None:
        self.repository = repository
        self.enqueue_alerts = enqueue_alerts

    def process(
        self,
        ticker: str,
        trading_date: date,
        *,
        trading_open_at: datetime,
        created_at: datetime,
    ) -> CloseBriefReport:
        context = self.repository.load_close_market_context(ticker, trading_date)
        if context is None:
            raise RuntimeError(
                f"close market context unavailable for {ticker.upper()} {trading_date}"
            )
        catalysts = self.repository.recent_catalysts(
            ticker,
            trading_open_at,
            limit=2,
        )
        relative = assess_relative_performance(context.snapshot)
        volume_assessment = assess_intraday_volume(context.volume)
        payload = build_close_message(
            context.snapshot,
            relative,
            context.volume,
            volume_assessment,
            catalysts,
        )
        event_key = f"{ticker.upper()}:{trading_date.isoformat()}:close"
        inserted = self.repository.record_alert(
            AlertRecord(
                event_key=event_key,
                ticker=ticker.upper(),
                alert_type="daily_close",
                created_at=created_at,
                payload=payload,
            ),
            enqueue=self.enqueue_alerts,
        )
        return CloseBriefReport(
            ticker=ticker.upper(),
            trading_date=trading_date.isoformat(),
            event_key=event_key,
            inserted=inserted,
            catalyst_count=len(catalysts),
            payload=payload,
        )
