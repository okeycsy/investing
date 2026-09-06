from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from investing_monitor.adapters.exchange_calendar import XNYSCalendar
from investing_monitor.domain.models import (
    InstrumentProfile,
    MarketCycle,
    MarketFrame,
    MarketSensitivity,
    MarketSession,
    MarketSnapshot,
    VolumeSnapshot,
)
from investing_monitor.domain.situation import build_market_sensitivity
from investing_monitor.runtime.tick import NEW_YORK


YAHOO_CHART_HOSTS = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
)
YAHOO_SPARK_HOSTS = (
    "https://query1.finance.yahoo.com/v7/finance/spark",
    "https://query2.finance.yahoo.com/v7/finance/spark",
)
YAHOO_COOKIE_URL = "https://fc.yahoo.com"
YAHOO_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}


class YahooMarketDataError(RuntimeError):
    pass


class StaleQuoteError(YahooMarketDataError):
    pass


class UnconfirmedQuoteError(YahooMarketDataError):
    pass


@dataclass(frozen=True)
class YahooBar:
    observed_at: datetime
    close: float
    volume: int


@dataclass(frozen=True)
class YahooChart:
    symbol: str
    interval: timedelta
    bars: tuple[YahooBar, ...]
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class YahooQuote:
    symbol: str
    observed_at: datetime
    current_price: float
    reference_close: float
    session: MarketSession
    extended_fallback: bool = False

    @property
    def change_pct(self) -> float:
        return _change_pct(self.current_price, self.reference_close)


class YahooQuoteClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        timeout: float = 15,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(YAHOO_HEADERS)
        self._sleep = sleeper
        self.timeout = timeout
        self._crumb = ""

    def fetch_many(
        self,
        symbols: Sequence[str],
        *,
        session: MarketSession,
    ) -> dict[str, YahooQuote]:
        normalized = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if not normalized:
            return {}
        for attempt in range(2):
            try:
                self._authenticate(force=attempt > 0)
                response = self.session.get(
                    YAHOO_QUOTE_URL,
                    params={"symbols": ",".join(normalized), "crumb": self._crumb},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt == 0:
                    self._sleep(15)
                    continue
                raise YahooMarketDataError(
                    f"Yahoo multi-quote unavailable: {exc}"
                ) from exc
            if response.status_code == 200:
                try:
                    return parse_quote_payload(response.json(), session=session)
                except (ValueError, TypeError, KeyError) as exc:
                    raise YahooMarketDataError(
                        f"Yahoo multi-quote returned an invalid payload: {exc}"
                    ) from exc
            if response.status_code == 429 and attempt == 0:
                self._sleep(_retry_after_seconds(response.headers.get("Retry-After")) or 15)
                continue
            if response.status_code in (401, 403) and attempt == 0:
                continue
            raise YahooMarketDataError(
                f"Yahoo multi-quote rejected with HTTP {response.status_code}"
            )
        raise YahooMarketDataError("Yahoo multi-quote authentication failed")

    def _authenticate(self, *, force: bool) -> None:
        if self._crumb and not force:
            return
        self._crumb = ""
        self.session.get(YAHOO_COOKIE_URL, timeout=self.timeout)
        response = self.session.get(YAHOO_CRUMB_URL, timeout=self.timeout)
        crumb = response.text.strip()
        if response.status_code != 200 or not crumb or crumb.startswith("{"):
            raise YahooMarketDataError(
                f"Yahoo crumb unavailable with HTTP {response.status_code}"
            )
        self._crumb = crumb


class YahooChartClient:
    def __init__(
        self,
        *,
        get: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        retry_delays: Sequence[float] = (0, 15, 30, 60),
        timeout: float = 15,
    ) -> None:
        self._get = get or requests.Session().get
        self._sleep = sleeper
        self.retry_delays = tuple(retry_delays)
        self.timeout = timeout

    def fetch(
        self,
        symbol: str,
        *,
        interval: str,
        range_: str,
        include_prepost: bool,
        retry_delays: Sequence[float] | None = None,
    ) -> YahooChart:
        symbol = symbol.upper()
        return self._fetch_parsed(
            urls=tuple(host.format(symbol=symbol) for host in YAHOO_CHART_HOSTS),
            params={
                "interval": interval,
                "range": range_,
                "includePrePost": str(include_prepost).lower(),
                "events": "div,splits",
            },
            label=f"chart for {symbol}",
            parser=lambda payload: parse_chart_payload(payload, symbol, interval),
            retry_delays=retry_delays,
        )

    def fetch_many(
        self,
        symbols: Sequence[str],
        *,
        interval: str,
        range_: str,
        include_prepost: bool,
        retry_delays: Sequence[float] | None = None,
    ) -> dict[str, YahooChart]:
        normalized = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if not normalized:
            return {}
        return self._fetch_parsed(
            urls=YAHOO_SPARK_HOSTS,
            params={
                "symbols": ",".join(normalized),
                "interval": interval,
                "range": range_,
                "includePrePost": str(include_prepost).lower(),
            },
            label=f"spark for {','.join(normalized)}",
            parser=lambda payload: parse_spark_payload(payload, interval),
            retry_delays=retry_delays,
        )

    def _fetch_parsed(
        self,
        *,
        urls: Sequence[str],
        params: Mapping[str, str],
        label: str,
        parser: Callable[[Mapping[str, Any]], Any],
        retry_delays: Sequence[float] | None,
    ):
        errors: list[str] = []
        retry_after: float | None = None
        delays = self.retry_delays if retry_delays is None else tuple(retry_delays)
        for attempt, delay in enumerate(delays):
            effective_delay = retry_after if retry_after is not None else delay
            retry_after = None
            if effective_delay:
                self._sleep(effective_delay)
            url = urls[attempt % len(urls)]
            try:
                response = self._get(
                    url,
                    params=params,
                    headers=YAHOO_HEADERS,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                errors.append(str(exc))
                continue

            if response.status_code == 200:
                try:
                    return parser(response.json())
                except (ValueError, TypeError, KeyError) as exc:
                    errors.append(f"invalid payload: {exc}")
                    continue
            if response.status_code in (429, 503):
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                errors.append(f"HTTP {response.status_code}")
                continue
            if response.status_code >= 500:
                errors.append(f"HTTP {response.status_code}")
                continue
            raise YahooMarketDataError(
                f"Yahoo {label} rejected with HTTP {response.status_code}"
            )
        detail = "; ".join(errors[-3:]) or "no response"
        raise YahooMarketDataError(f"Yahoo {label} unavailable: {detail}")


class YahooMarketDataAdapter:
    def __init__(
        self,
        client: YahooChartClient,
        calendar: XNYSCalendar,
        profile: InstrumentProfile,
        *,
        quote_client: YahooQuoteClient | None = None,
        interval: timedelta = timedelta(minutes=5),
        regular_freshness: timedelta = timedelta(minutes=5),
        extended_freshness: timedelta = timedelta(minutes=15),
    ) -> None:
        self.client = client
        self.calendar = calendar
        self.profile = profile
        self.quote_client = quote_client
        self.interval = interval
        self.regular_freshness = regular_freshness
        self.extended_freshness = extended_freshness

    def fetch_sensitivity(self, now: datetime) -> MarketSensitivity:
        now = _utc(now)
        symbols = (
            self.profile.ticker,
            self.profile.benchmark,
            *self.profile.peers,
        )
        charts = self.client.fetch_many(
            symbols,
            interval="1d",
            range_="6mo",
            include_prepost=False,
            retry_delays=(0, 5),
        )
        closes = {
            symbol: _daily_closes(charts[symbol])
            for symbol in symbols
            if symbol in charts
        }
        model = build_market_sensitivity(
            ticker=self.profile.ticker,
            benchmark_symbol=self.profile.benchmark,
            peer_symbols=self.profile.peers,
            daily_closes=closes,
            calculated_at=now,
        )
        if model.benchmark_beta is None and model.peer_beta is None:
            raise YahooMarketDataError(
                "Yahoo daily history is insufficient for a sensitivity model"
            )
        return model

    def fetch_cycle(
        self,
        now: datetime,
        *,
        last_observed_at: datetime | None,
    ) -> MarketCycle:
        now = _utc(now)
        trading_date = now.astimezone(NEW_YORK).date()
        if not self.calendar.is_trading_day(trading_date):
            raise YahooMarketDataError("market task requested on a non-trading day")
        if not self.calendar.is_extended_session(now):
            raise YahooMarketDataError("market task requested outside 04:00-20:00 ET")

        primary = self.client.fetch(
            self.profile.ticker,
            interval="5m",
            range_="1mo",
            include_prepost=True,
        )
        session = self.calendar.session_at(now)
        comparison_quotes: dict[str, YahooQuote] | None = None
        comparisons: dict[str, YahooChart | None] = {}
        if self.quote_client is not None:
            comparison_quotes = self.quote_client.fetch_many(
                (self.profile.ticker, self.profile.benchmark, *self.profile.peers),
                session=session,
            )
        else:
            symbols = (self.profile.benchmark, *self.profile.peers)
            try:
                charts = self.client.fetch_many(
                    symbols,
                    interval="5m",
                    range_="1mo",
                    include_prepost=True,
                )
            except YahooMarketDataError:
                charts = self._individual_fallback(symbols)
            comparisons = {symbol: charts.get(symbol) for symbol in symbols}

        current_bars = self._current_session_bars(primary, trading_date, now)
        if not current_bars:
            raise YahooMarketDataError(f"no current-session bars for {self.profile.ticker}")
        latest = current_bars[-1]
        source_age = self._validate_freshness(latest, now)
        reference_close = self._reference_close(primary, trading_date)
        self._validate_large_move(latest, reference_close, comparison_quotes)
        comparison_references = {
            symbol: self._optional_reference_close(chart, trading_date)
            for symbol, chart in comparisons.items()
        }
        cumulative = self._cumulative_regular_volume(current_bars, trading_date)

        replay_bars = self._bars_after_cursor(
            current_bars,
            trading_date,
            last_observed_at,
        )
        frames = tuple(
            self._frame(
                bar,
                trading_date,
                reference_close,
                cumulative.get(bar.observed_at, 0),
                comparisons,
                comparison_references,
                comparison_quotes,
            )
            for bar in replay_bars
        )
        volume = self._volume_snapshot(
            primary,
            current_bars,
            latest,
            trading_date,
        )
        return MarketCycle(
            ticker=self.profile.ticker,
            trading_date=trading_date,
            frames=frames,
            volume=volume,
            source_age_seconds=source_age,
        )

    def _validate_large_move(
        self,
        latest: YahooBar,
        reference_close: float,
        quotes: Mapping[str, YahooQuote] | None,
    ) -> None:
        if abs(_change_pct(latest.close, reference_close)) < 30:
            return
        confirmation = (quotes or {}).get(self.profile.ticker)
        if confirmation is None:
            raise UnconfirmedQuoteError(
                f"abnormal {self.profile.ticker} move has no independent confirmation"
            )
        price_difference = abs(confirmation.current_price - latest.close) / latest.close
        if price_difference > 0.01:
            raise UnconfirmedQuoteError(
                f"abnormal {self.profile.ticker} move disagrees with multi-quote"
            )

    def _individual_fallback(self, symbols: Sequence[str]) -> dict[str, YahooChart]:
        charts: dict[str, YahooChart] = {}
        for index, symbol in enumerate(symbols):
            try:
                charts[symbol] = self.client.fetch(
                    symbol,
                    interval="5m",
                    range_="1mo" if index == 0 else "5d",
                    include_prepost=True,
                    retry_delays=(0,),
                )
            except YahooMarketDataError:
                if index == 0:
                    raise
        return charts

    def _frame(
        self,
        bar: YahooBar,
        trading_date: date,
        reference_close: float,
        cumulative_volume: int,
        comparisons: Mapping[str, YahooChart | None],
        comparison_references: Mapping[str, float | None],
        comparison_quotes: Mapping[str, YahooQuote] | None,
    ) -> MarketFrame:
        if comparison_quotes is not None:
            comparison_changes = {
                symbol: None if quote.extended_fallback else quote.change_pct
                for symbol, quote in comparison_quotes.items()
            }
        else:
            comparison_changes = {
                symbol: self._comparison_change(
                    chart,
                    comparison_references.get(symbol),
                    trading_date,
                    bar.observed_at,
                )
                for symbol, chart in comparisons.items()
            }
        snapshot = MarketSnapshot(
            ticker=self.profile.ticker,
            trading_date=trading_date,
            observed_at=bar.observed_at,
            session=self.calendar.session_at(bar.observed_at),
            change_pct=_change_pct(bar.close, reference_close),
            benchmark_change_pct=comparison_changes.get(self.profile.benchmark),
            benchmark_symbol=self.profile.benchmark,
            peer_changes={
                peer: comparison_changes.get(peer) for peer in self.profile.peers
            },
        )
        return MarketFrame(
            snapshot=snapshot,
            close_price=bar.close,
            reference_close=reference_close,
            cumulative_volume=cumulative_volume,
        )

    def _reference_close(self, chart: YahooChart, trading_date: date) -> float:
        previous_date = self.calendar.previous_trading_day(trading_date)
        bars = self._regular_bars(chart, previous_date)
        if not bars:
            raise YahooMarketDataError(
                f"previous regular close unavailable for {chart.symbol}"
            )
        return bars[-1].close

    def _optional_reference_close(
        self,
        chart: YahooChart | None,
        trading_date: date,
    ) -> float | None:
        if chart is None:
            return None
        try:
            return self._reference_close(chart, trading_date)
        except YahooMarketDataError:
            return None

    def _comparison_change(
        self,
        chart: YahooChart | None,
        reference_close: float | None,
        trading_date: date,
        at: datetime,
    ) -> float | None:
        if chart is None or reference_close is None:
            return None
        eligible = [
            bar
            for bar in chart.bars
            if bar.observed_at.astimezone(NEW_YORK).date() == trading_date
            and bar.observed_at <= at
        ]
        if not eligible:
            return None
        latest = eligible[-1]
        if at - latest.observed_at > timedelta(minutes=15):
            return None
        return _change_pct(latest.close, reference_close)

    def _current_session_bars(
        self,
        chart: YahooChart,
        trading_date: date,
        now: datetime,
    ) -> list[YahooBar]:
        return [
            bar
            for bar in chart.bars
            if bar.observed_at.astimezone(NEW_YORK).date() == trading_date
            and bar.observed_at <= now
            and self.calendar.session_at(bar.observed_at) is not MarketSession.CLOSED
        ]

    def _regular_bars(self, chart: YahooChart, trading_date: date) -> list[YahooBar]:
        window = self.calendar.window(trading_date)
        return [
            bar
            for bar in chart.bars
            if window.open_at <= bar.observed_at < window.close_at
        ]

    def _validate_freshness(self, latest: YahooBar, now: datetime) -> int:
        age = max(0, int((now - (latest.observed_at + self.interval)).total_seconds()))
        session = self.calendar.session_at(now)
        allowed = (
            self.regular_freshness
            if session is MarketSession.REGULAR
            else self.extended_freshness
        )
        if age > int(allowed.total_seconds()):
            raise StaleQuoteError(
                f"latest {self.profile.ticker} quote is stale by {age} seconds"
            )
        return age

    def _cumulative_regular_volume(
        self,
        current_bars: Sequence[YahooBar],
        trading_date: date,
    ) -> dict[datetime, int]:
        window = self.calendar.window(trading_date)
        running = 0
        cumulative: dict[datetime, int] = {}
        for bar in current_bars:
            if window.open_at <= bar.observed_at < window.close_at:
                running += max(0, bar.volume)
            cumulative[bar.observed_at] = running
        return cumulative

    def _bars_after_cursor(
        self,
        current_bars: Sequence[YahooBar],
        trading_date: date,
        last_observed_at: datetime | None,
    ) -> tuple[YahooBar, ...]:
        if last_observed_at is None:
            return tuple(current_bars)
        cursor = _utc(last_observed_at)
        if cursor.astimezone(NEW_YORK).date() != trading_date:
            return tuple(current_bars)
        return tuple(bar for bar in current_bars if bar.observed_at > cursor)

    def _volume_snapshot(
        self,
        chart: YahooChart,
        current_bars: Sequence[YahooBar],
        latest: YahooBar,
        trading_date: date,
    ) -> VolumeSnapshot | None:
        session = self.calendar.session_at(latest.observed_at)
        if session is MarketSession.PRE:
            return None
        window = self.calendar.window(trading_date)
        latest_at = min(latest.observed_at, window.close_at - self.interval)
        offset = max(timedelta(0), latest_at - window.open_at)
        observed = sum(
            max(0, bar.volume)
            for bar in current_bars
            if window.open_at <= bar.observed_at <= latest_at
        )

        grouped: dict[date, list[YahooBar]] = {}
        for bar in chart.bars:
            bar_date = bar.observed_at.astimezone(NEW_YORK).date()
            if bar_date >= trading_date or not self.calendar.is_trading_day(bar_date):
                continue
            grouped.setdefault(bar_date, []).append(bar)

        baselines: list[int] = []
        for prior_date in sorted(grouped, reverse=True):
            prior_window = self.calendar.window(prior_date)
            cutoff = min(
                prior_window.open_at + offset,
                prior_window.close_at - self.interval,
            )
            cumulative = sum(
                max(0, bar.volume)
                for bar in grouped[prior_date]
                if prior_window.open_at <= bar.observed_at <= cutoff
            )
            if cumulative > 0:
                baselines.append(cumulative)
            if len(baselines) == 20:
                break
        if not baselines or observed <= 0:
            return None
        return VolumeSnapshot(
            observed_volume=observed,
            expected_volume=int(sum(baselines) / len(baselines)),
            baseline_sessions=len(baselines),
            lookback_sessions=20,
        )


def parse_chart_payload(payload: Mapping[str, Any], symbol: str, interval: str) -> YahooChart:
    chart = payload["chart"]
    if chart.get("error"):
        raise ValueError(str(chart["error"]))
    results = chart.get("result") or []
    if not results:
        raise ValueError("chart result is empty")
    result = results[0]
    timestamps = result.get("timestamp") or []
    quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quotes.get("close") or []
    volumes = quotes.get("volume") or []
    bars: dict[datetime, YahooBar] = {}
    for index, raw_timestamp in enumerate(timestamps):
        close = closes[index] if index < len(closes) else None
        if close is None or float(close) <= 0:
            continue
        observed_at = datetime.fromtimestamp(int(raw_timestamp), timezone.utc)
        raw_volume = volumes[index] if index < len(volumes) else 0
        bars[observed_at] = YahooBar(
            observed_at=observed_at,
            close=float(close),
            volume=max(0, int(raw_volume or 0)),
        )
    if not bars:
        raise ValueError("chart contains no valid bars")
    seconds = _interval_seconds(interval)
    return YahooChart(
        symbol=symbol.upper(),
        interval=timedelta(seconds=seconds),
        bars=tuple(bars[key] for key in sorted(bars)),
        metadata=result.get("meta") or {},
    )


def parse_spark_payload(
    payload: Mapping[str, Any],
    interval: str,
) -> dict[str, YahooChart]:
    results = (payload.get("spark") or {}).get("result") or []
    charts: dict[str, YahooChart] = {}
    for item in results:
        symbol = str(item.get("symbol") or "").upper()
        responses = item.get("response") or []
        if not symbol or not responses:
            continue
        chart_payload = {"chart": {"result": [responses[0]], "error": None}}
        try:
            charts[symbol] = parse_chart_payload(chart_payload, symbol, interval)
        except (ValueError, TypeError, KeyError):
            continue
    if not charts:
        raise ValueError("spark result contains no valid charts")
    return charts


def parse_quote_payload(
    payload: Mapping[str, Any],
    *,
    session: MarketSession,
) -> dict[str, YahooQuote]:
    results = (payload.get("quoteResponse") or {}).get("result") or []
    quotes: dict[str, YahooQuote] = {}
    for item in results:
        symbol = str(item.get("symbol") or "").upper()
        selected = _select_quote_fields(item, session)
        if not symbol or selected is None:
            continue
        current, reference, raw_timestamp, fallback = selected
        if current <= 0 or reference <= 0 or not raw_timestamp:
            continue
        quotes[symbol] = YahooQuote(
            symbol=symbol,
            observed_at=datetime.fromtimestamp(int(raw_timestamp), timezone.utc),
            current_price=current,
            reference_close=reference,
            session=session,
            extended_fallback=fallback,
        )
    return quotes


def _select_quote_fields(
    item: Mapping[str, Any],
    session: MarketSession,
) -> tuple[float, float, int, bool] | None:
    regular_price = _positive_float(item.get("regularMarketPrice"))
    regular_previous = _positive_float(item.get("regularMarketPreviousClose"))
    regular_time = int(item.get("regularMarketTime") or 0)
    if session is MarketSession.PRE:
        reference = regular_price
        current = _positive_float(item.get("preMarketPrice"))
        raw_timestamp = int(item.get("preMarketTime") or 0)
        if current is None:
            current, raw_timestamp = regular_price, regular_time
            fallback = True
        else:
            fallback = False
    elif session is MarketSession.POST:
        reference = regular_previous
        current = _positive_float(item.get("postMarketPrice"))
        raw_timestamp = int(item.get("postMarketTime") or 0)
        if current is None:
            current, raw_timestamp = regular_price, regular_time
            fallback = True
        else:
            fallback = False
    elif session is MarketSession.REGULAR:
        reference = regular_previous
        current = regular_price
        raw_timestamp = regular_time
        fallback = False
    else:
        return None
    if current is None or reference is None:
        return None
    return current, reference, raw_timestamp, fallback


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _change_pct(current: float, reference: float) -> float:
    return (current - reference) / reference * 100


def _daily_closes(chart: YahooChart) -> dict[date, float]:
    closes: dict[date, float] = {}
    for bar in chart.bars:
        closes[bar.observed_at.astimezone(NEW_YORK).date()] = bar.close
    return closes


def _interval_seconds(interval: str) -> int:
    suffix = interval[-1]
    value = int(interval[:-1])
    factors = {"m": 60, "h": 3600, "d": 86400}
    if suffix not in factors:
        raise ValueError(f"unsupported Yahoo interval: {interval}")
    return value * factors[suffix]


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value).astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return None
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Yahoo timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
