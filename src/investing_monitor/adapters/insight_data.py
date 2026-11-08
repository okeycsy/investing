"""Data sources for the insight path.

* daily closes        — the existing Yahoo spark client (same call as sensitivity)
* option chain        — yfinance (lazy import; already installed in production)
* short interest      — yfinance quoteSummary fields (exchange-reported, bi-monthly)
* next earnings date  — yfinance earnings dates / calendar
* XBRL company facts  — data.sec.gov companyfacts JSON with the SEC contact header

Every fetch raises on failure; the application layer records the error and
degrades to a partial bundle instead of hiding the gap.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from math import isfinite
from typing import Any

import requests

from investing_monitor.adapters.yahoo_market_data import YahooChartClient, _daily_closes
from investing_monitor.domain.insights import OptionQuote


class InsightDataError(RuntimeError):
    pass


class YahooInsightAdapter:
    def __init__(
        self,
        chart_client: YahooChartClient,
        *,
        ticker_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self.chart_client = chart_client
        self._ticker_factory = ticker_factory

    def fetch_daily_closes(self, symbols: Sequence[str]) -> dict[str, dict[date, float]]:
        normalized = tuple(dict.fromkeys(symbol.upper() for symbol in symbols if symbol))
        charts = self.chart_client.fetch_many(
            normalized,
            interval="1d",
            range_="7mo",
            include_prepost=False,
            retry_delays=(0, 5),
        )
        return {symbol: _daily_closes(chart) for symbol, chart in charts.items()}

    def fetch_option_quotes(self, ticker: str, *, max_expiries: int = 8) -> tuple[OptionQuote, ...]:
        yf_ticker = self._yf(ticker)
        try:
            expiries = list(yf_ticker.options or [])[:max_expiries]
        except Exception as exc:  # noqa: BLE001
            raise InsightDataError(f"option expiries unavailable: {exc}") from exc
        quotes: list[OptionQuote] = []
        for raw in expiries:
            try:
                chain = yf_ticker.option_chain(raw)
            except Exception as exc:  # noqa: BLE001
                raise InsightDataError(f"option chain {raw} unavailable: {exc}") from exc
            expiration = date.fromisoformat(str(raw)[:10])
            for option_type, frame in (("call", chain.calls), ("put", chain.puts)):
                for row in frame.to_dict("records"):
                    quotes.append(
                        OptionQuote(
                            expiration=expiration,
                            strike=float(row.get("strike") or 0),
                            option_type=option_type,
                            open_interest=_int(row.get("openInterest")),
                            volume=_int(row.get("volume")),
                            bid=_float(row.get("bid")),
                            ask=_float(row.get("ask")),
                            last=_float(row.get("lastPrice")),
                            implied_volatility=_float(row.get("impliedVolatility")),
                        )
                    )
        return tuple(quotes)

    def fetch_short_interest_fields(self, ticker: str) -> dict[str, Any]:
        yf_ticker = self._yf(ticker)
        try:
            info = yf_ticker.get_info()
        except Exception as exc:  # noqa: BLE001
            raise InsightDataError(f"quote summary unavailable: {exc}") from exc
        keys = (
            "sharesShort", "sharesShortPriorMonth", "shortPercentOfFloat",
            "dateShortInterest", "shortRatio", "floatShares",
        )
        return {key: info.get(key) for key in keys if info.get(key) is not None}

    def fetch_next_earnings(self, ticker: str, *, today: date) -> date | None:
        yf_ticker = self._yf(ticker)
        candidates: list[date] = []
        try:
            frame = yf_ticker.get_earnings_dates(limit=8)
            if frame is not None:
                for stamp in frame.index:
                    value = stamp.date() if hasattr(stamp, "date") else None
                    if value and value >= today:
                        candidates.append(value)
        except Exception:  # noqa: BLE001
            pass
        if not candidates:
            try:
                calendar = yf_ticker.calendar or {}
                for value in calendar.get("Earnings Date", []) or []:
                    value = value.date() if hasattr(value, "date") and not isinstance(value, date) else value
                    if isinstance(value, date) and value >= today:
                        candidates.append(value)
            except Exception:  # noqa: BLE001
                pass
        return min(candidates) if candidates else None

    def _yf(self, ticker: str):
        if self._ticker_factory is not None:
            return self._ticker_factory(ticker)
        try:
            import yfinance as yf  # lazy: only the insight path needs it
        except ImportError as exc:  # pragma: no cover
            raise InsightDataError("yfinance is not installed") from exc
        return yf.Ticker(ticker.upper())


class SecCompanyFactsClient:
    """`data.sec.gov/api/xbrl/companyfacts` with the same etiquette as the
    submissions client (contact header, 0.2 s spacing, no retries on 403)."""

    def __init__(
        self,
        *,
        get: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        minimum_interval: float = 0.2,
        timeout: float = 30,
    ) -> None:
        self._get = get or requests.Session().get
        self._sleep = sleeper
        self._monotonic = monotonic
        self.minimum_interval = minimum_interval
        self.timeout = timeout
        self._last_request_at: float | None = None

    def fetch(self, cik: str, *, contact: str) -> dict[str, Any]:
        if not cik:
            raise InsightDataError("CIK is unavailable")
        if not contact:
            raise InsightDataError("SEC contact is unavailable")
        if self._last_request_at is not None:
            elapsed = self._monotonic() - self._last_request_at
            if elapsed < self.minimum_interval:
                self._sleep(self.minimum_interval - elapsed)
        self._last_request_at = self._monotonic()
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"
        headers = {
            "User-Agent": f"investing-monitor/2.0 {contact}",
            "From": contact,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
        response = None
        for attempt in range(2):
            try:
                response = self._get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as exc:
                raise InsightDataError(f"companyfacts request failed: {exc}") from exc
            if response.status_code == 429 and attempt == 0:
                self._sleep(2.0)  # SEC asks for ≤10 req/s; one polite retry
                continue
            break
        if response is None or response.status_code != 200:
            # 403 is SEC's automated-traffic block; there is no mirror for XBRL,
            # so the caller records the gap and the quarterly alert waits for
            # the next SEC poll instead of failing the tick.
            raise InsightDataError(f"companyfacts rejected with HTTP {response.status_code if response is not None else 'n/a'}")
        try:
            facts = response.json().get("facts", {})
        except ValueError as exc:
            raise InsightDataError("companyfacts payload is not JSON") from exc
        merged = dict(facts.get("us-gaap", {}))
        for key, value in facts.get("dei", {}).items():
            merged.setdefault(key, value)
        return merged


def _int(value: Any) -> int:
    try:
        number = float(value)
        return int(number) if isfinite(number) else 0
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        number = float(value)
        return number if isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def utc_today(now: datetime | None = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
