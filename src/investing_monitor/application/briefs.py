from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import prod

from investing_monitor.domain.models import (
    Catalyst,
    CloseMarketContext,
    MarketSession,
    MarketSensitivity,
    MarketSnapshot,
    ThesisImpact,
)
from investing_monitor.domain.policies import (
    assess_intraday_volume,
    assess_market_situation,
    assess_relative_performance,
)
from investing_monitor.ports.repository import AlertRecord, MonitorRepository
from investing_monitor.presentation.close_messages import build_close_message
from investing_monitor.presentation.weekly_messages import build_weekly_message


class WeeklyBriefUnavailable(RuntimeError):
    pass


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
        sensitivity: MarketSensitivity | None = None,
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
        relative = assess_relative_performance(
            context.snapshot,
            sensitivity=sensitivity,
        )
        volume_assessment = assess_intraday_volume(context.volume)
        situation = assess_market_situation(context.snapshot, relative)
        payload = build_close_message(
            context.snapshot,
            relative,
            context.volume,
            volume_assessment,
            catalysts,
            situation,
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


@dataclass(frozen=True)
class WeeklyBriefReport:
    ticker: str
    period_start: str
    period_end: str
    event_key: str
    inserted: bool
    market_sessions: int
    strengthening_count: int
    risk_count: int
    upcoming_event_count: int
    payload: dict

    def as_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "event_key": self.event_key,
            "inserted": self.inserted,
            "market_sessions": self.market_sessions,
            "strengthening_count": self.strengthening_count,
            "risk_count": self.risk_count,
            "upcoming_event_count": self.upcoming_event_count,
            "message": self.payload,
        }


class WeeklyBriefService:
    def __init__(
        self,
        repository: MonitorRepository,
        *,
        enqueue_alerts: bool = True,
        minimum_market_sessions: int = 3,
    ) -> None:
        self.repository = repository
        self.enqueue_alerts = enqueue_alerts
        self.minimum_market_sessions = minimum_market_sessions

    def process(
        self,
        ticker: str,
        period_start: date,
        period_end: date,
        *,
        evidence_since: datetime,
        created_at: datetime,
    ) -> WeeklyBriefReport:
        contexts = self.repository.load_close_market_contexts(
            ticker,
            period_start,
            period_end,
        )
        if len(contexts) < self.minimum_market_sessions:
            raise WeeklyBriefUnavailable(
                f"weekly market context has only {len(contexts)} completed sessions"
            )

        snapshot = _weekly_snapshot(contexts)
        latest_volume = contexts[-1].volume
        catalysts = _unique_catalysts(
            self.repository.recent_catalysts(ticker, evidence_since, limit=30)
        )
        strengthening = tuple(
            item
            for item in catalysts
            if item.confidence.lower() == "high"
            and item.impact is ThesisImpact.STRENGTHEN
        )
        risks = tuple(
            item
            for item in catalysts
            if item.confidence.lower() == "high"
            and item.impact in {ThesisImpact.RISK, ThesisImpact.DAMAGE}
        )
        next_period_start = period_start + timedelta(days=7)
        upcoming_events = self.repository.upcoming_official_events(
            ticker,
            next_period_start,
            next_period_start + timedelta(days=6),
            limit=3,
        )
        payload = build_weekly_message(
            snapshot,
            assess_relative_performance(snapshot),
            latest_volume,
            assess_intraday_volume(latest_volume),
            strengthening,
            risks,
            upcoming_events,
            period_start=period_start,
            period_end=period_end,
            session_count=len(contexts),
        )
        iso_year, iso_week, _ = period_end.isocalendar()
        event_key = f"{ticker.upper()}:{iso_year}-W{iso_week:02d}:weekly"
        inserted = self.repository.record_alert(
            AlertRecord(
                event_key=event_key,
                ticker=ticker.upper(),
                alert_type="weekly_review",
                created_at=created_at,
                payload=payload,
            ),
            enqueue=self.enqueue_alerts,
        )
        return WeeklyBriefReport(
            ticker=ticker.upper(),
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            event_key=event_key,
            inserted=inserted,
            market_sessions=len(contexts),
            strengthening_count=len(strengthening),
            risk_count=len(risks),
            upcoming_event_count=len(upcoming_events),
            payload=payload,
        )


def _weekly_snapshot(contexts: tuple[CloseMarketContext, ...]) -> MarketSnapshot:
    snapshots = tuple(context.snapshot for context in contexts)
    latest = snapshots[-1]
    benchmark_changes = tuple(item.benchmark_change_pct for item in snapshots)
    benchmark_change = (
        _compound(tuple(float(value) for value in benchmark_changes if value is not None))
        if all(value is not None for value in benchmark_changes)
        else None
    )
    peer_symbols = tuple(sorted(latest.peer_changes))
    peer_changes = {}
    for symbol in peer_symbols:
        values = tuple(item.peer_changes.get(symbol) for item in snapshots)
        peer_changes[symbol] = (
            _compound(tuple(float(value) for value in values if value is not None))
            if all(value is not None for value in values)
            else None
        )
    return MarketSnapshot(
        ticker=latest.ticker,
        trading_date=latest.trading_date,
        observed_at=latest.observed_at,
        session=MarketSession.CLOSED,
        change_pct=_compound(tuple(item.change_pct for item in snapshots)),
        benchmark_change_pct=benchmark_change,
        benchmark_symbol=latest.benchmark_symbol,
        peer_changes=peer_changes,
    )


def _compound(changes: tuple[float, ...]) -> float:
    return (prod(1.0 + change / 100.0 for change in changes) - 1.0) * 100.0


def _unique_catalysts(catalysts: list[Catalyst]) -> tuple[Catalyst, ...]:
    unique = {}
    for catalyst in catalysts:
        unique.setdefault(catalyst.canonical_id, catalyst)
    return tuple(unique.values())
