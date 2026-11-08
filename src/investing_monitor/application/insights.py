"""Insight orchestration: fetch → interpret → remember → render-ready lines.

Runs only on the close, weekly and SEC due-tasks, never on the 5-minute market
tick, so the extra Yahoo/SEC calls cost a few requests a day.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from investing_monitor.domain.evidence import EvidenceProfile
from investing_monitor.domain.insights import (
    EarningsQuality,
    InsightBundle,
    InsightLine,
    InsightProfile,
    convertible_watch,
    earnings_quality,
    options_insight,
    proxy_moves,
    rank_insight_lines,
    realized_volatility_pct,
    residual_drift,
    short_interest_insight,
    trailing_change_pct,
    weekly_insight_lines,
)
from investing_monitor.domain.models import InstrumentProfile, MarketSensitivity, Position
from investing_monitor.ports.repository import AlertRecord
from investing_monitor.presentation.insight_messages import build_earnings_quality_message

QUALITY_ALERT_TYPE = "earnings_quality"
QUALITY_MAX_AGE_DAYS = 120


@dataclass(frozen=True)
class InsightReport:
    lines: tuple[InsightLine, ...]
    bundle: InsightBundle | None
    errors: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "insight_lines": len(self.lines),
            "insight_kinds": [line.kind for line in self.lines],
            "insight_errors": list(self.errors),
        }


@dataclass(frozen=True)
class QualityCheckpointReport:
    period_end: str
    event_key: str
    inserted: bool
    verdict_count: int
    skipped: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "period_end": self.period_end,
            "event_key": self.event_key,
            "inserted": self.inserted,
            "verdict_count": self.verdict_count,
            "skipped": self.skipped,
        }


class InsightService:
    def __init__(
        self,
        repository,
        market,
        facts_client,
        *,
        profile: InstrumentProfile,
        evidence_profile: EvidenceProfile,
        insight_profile: InsightProfile,
        position: Position | None = None,
        enqueue_alerts: bool = False,
    ) -> None:
        self.repository = repository
        self.market = market
        self.facts_client = facts_client
        self.profile = profile
        self.evidence_profile = evidence_profile
        self.insight_profile = insight_profile
        self.position = position
        self.enqueue_alerts = enqueue_alerts

    # ───────────────────── bundle ─────────────────────
    def build_bundle(
        self,
        trading_date: date,
        *,
        sensitivity: MarketSensitivity | None,
    ) -> InsightBundle | None:
        ticker = self.profile.ticker.upper()
        errors: list[str] = []
        symbols = [
            ticker,
            self.profile.benchmark,
            *self.profile.peers,
            *(symbol for symbol, _ in self.insight_profile.customer_proxies),
        ]
        try:
            closes = self.market.fetch_daily_closes(symbols)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"daily closes: {exc}")
            closes = {}
        own = closes.get(ticker, {})
        if not own:
            return None
        last_date = max(own)
        close = own[last_date]
        change = trailing_change_pct(own, 1)

        next_earnings = None
        try:
            next_earnings = self.market.fetch_next_earnings(ticker, today=trading_date)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"earnings date: {exc}")

        options = None
        try:
            quotes = self.market.fetch_option_quotes(ticker)
            options = options_insight(quotes, spot=close, as_of=trading_date, next_earnings=next_earnings)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"options: {exc}")

        short = None
        try:
            short = short_interest_insight(self.market.fetch_short_interest_fields(ticker))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"short interest: {exc}")

        drift = residual_drift(
            ticker=ticker,
            daily_closes=closes,
            sensitivity=sensitivity,
            benchmark_symbol=self.profile.benchmark,
            peer_symbols=self.profile.peers,
        )
        convertibles = tuple(
            watch
            for watch in (
                convertible_watch(note, own) for note in self.insight_profile.convertibles
            )
            if watch is not None
        )
        return InsightBundle(
            ticker=ticker,
            trading_date=last_date,
            close=close,
            change_pct=change or 0.0,
            realized_vol_20d_pct=realized_volatility_pct(own),
            drift=drift,
            options=options,
            convertibles=convertibles,
            short_interest=short,
            proxies=proxy_moves(self.insight_profile.customer_proxies, closes),
            ticker_5d_pct=trailing_change_pct(own, 5),
            next_earnings=next_earnings,
            errors=tuple(errors),
        )

    def close_report(
        self,
        trading_date: date,
        *,
        sensitivity: MarketSensitivity | None,
    ) -> InsightReport:
        bundle = self.build_bundle(trading_date, sensitivity=sensitivity)
        if bundle is None:
            return InsightReport(lines=(), bundle=None, errors=("daily closes unavailable",))
        previous = self.repository.load_insight_snapshot(
            bundle.ticker,
            before=bundle.trading_date,
        )
        lines = rank_insight_lines(
            bundle,
            previous=previous,
            profile=self.insight_profile,
            position=self.position,
        )
        self.repository.save_insight_snapshot(
            bundle.ticker,
            bundle.trading_date,
            bundle.snapshot(),
        )
        return InsightReport(lines=tuple(lines), bundle=bundle, errors=bundle.errors)

    def weekly_lines(
        self,
        period_end: date,
        *,
        sensitivity: MarketSensitivity | None,
    ) -> tuple[str, ...]:
        bundle = self.build_bundle(period_end, sensitivity=sensitivity)
        if bundle is None:
            return ()
        week_ago = self.repository.load_insight_snapshot(
            bundle.ticker,
            before=period_end - timedelta(days=5),
        )
        return tuple(weekly_insight_lines(bundle, week_ago=week_ago))

    # ───────────────────── quarterly checkpoint ─────────────────────
    def quarterly_checkpoint(self, now: datetime) -> QualityCheckpointReport:
        """Alert once per newly reported quarter with the XBRL quality table."""
        ticker = self.profile.ticker.upper()
        facts = self.facts_client.fetch(
            self.evidence_profile.cik,
            contact=self.evidence_profile.sec_contact,
        )
        quality = earnings_quality(facts, cik=self.evidence_profile.cik)
        if quality is None:
            return QualityCheckpointReport("", "", False, 0, skipped="xbrl table unavailable")
        period_end = quality.latest.period_end
        if (now.date() - period_end).days > QUALITY_MAX_AGE_DAYS:
            return QualityCheckpointReport(
                period_end.isoformat(), "", False, len(quality.verdict),
                skipped="latest quarter is older than the alert window",
            )
        event_key = f"{ticker}:{period_end.isoformat()}:{QUALITY_ALERT_TYPE}"
        if self.repository.alert_exists(event_key):
            return QualityCheckpointReport(period_end.isoformat(), event_key, False, len(quality.verdict), skipped="already alerted")
        filing_url = quality.source_url or f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={self.evidence_profile.cik}&type=10-Q"
        payload = build_earnings_quality_message(
            ticker,
            quality,
            created_at=now,
            filing_url=filing_url,
        )
        inserted = self.repository.record_alert(
            AlertRecord(
                event_key=event_key,
                ticker=ticker,
                alert_type=QUALITY_ALERT_TYPE,
                created_at=now,
                payload=payload,
            ),
            enqueue=self.enqueue_alerts,
        )
        return QualityCheckpointReport(period_end.isoformat(), event_key, inserted, len(quality.verdict))
