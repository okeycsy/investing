from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.config import load_instrument_profile
from investing_monitor.adapters.exchange_calendar import XNYSCalendar
from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.adapters.yahoo_market_data import (
    StaleQuoteError,
    UnconfirmedQuoteError,
    YahooBar,
    YahooChart,
    YahooChartClient,
    YahooMarketDataAdapter,
    YahooMarketDataError,
    YahooQuote,
    parse_quote_payload,
)
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import (
    InstrumentProfile,
    MarketCycle,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    VolumeSnapshot,
)
from investing_monitor.runtime.tick import NEW_YORK


TRADING_DATE = date(2026, 9, 2)
PROFILE = InstrumentProfile("VRT", "SOXX", ("ETN", "GEV", "NVT"))


def et(value: date, hour: int, minute: int) -> datetime:
    return datetime(
        value.year,
        value.month,
        value.day,
        hour,
        minute,
        tzinfo=NEW_YORK,
    ).astimezone(timezone.utc)


def make_chart(symbol: str, bars: list[YahooBar]) -> YahooChart:
    return YahooChart(
        symbol=symbol,
        interval=timedelta(minutes=5),
        bars=tuple(sorted(bars, key=lambda item: item.observed_at)),
        metadata={},
    )


def bar(value: date, hour: int, minute: int, close: float, volume: int = 0) -> YahooBar:
    return YahooBar(et(value, hour, minute), close, volume)


class FakeChartClient:
    def __init__(self, charts: dict[str, YahooChart]) -> None:
        self.charts = charts
        self.calls: list[str] = []

    def fetch(self, symbol: str, **_kwargs) -> YahooChart:
        self.calls.append(symbol)
        if symbol not in self.charts:
            raise YahooMarketDataError(f"missing {symbol}")
        return self.charts[symbol]

    def fetch_many(self, symbols, **_kwargs) -> dict[str, YahooChart]:
        self.calls.append(",".join(symbols))
        return {symbol: self.charts[symbol] for symbol in symbols if symbol in self.charts}


class FakeQuoteClient:
    def __init__(self, quotes: dict[str, YahooQuote]) -> None:
        self.quotes = quotes

    def fetch_many(self, symbols, **_kwargs) -> dict[str, YahooQuote]:
        return {symbol: self.quotes[symbol] for symbol in symbols if symbol in self.quotes}


def prior_sessions(calendar: XNYSCalendar, count: int = 20) -> list[date]:
    sessions: list[date] = []
    candidate = TRADING_DATE
    for _ in range(count):
        candidate = calendar.previous_trading_day(candidate)
        sessions.append(candidate)
    return sessions


def history_bars(calendar: XNYSCalendar) -> list[YahooBar]:
    bars: list[YahooBar] = []
    for session_date in prior_sessions(calendar):
        bars.extend(
            [
                bar(session_date, 9, 30, 99.0, 100),
                bar(session_date, 10, 0, 99.5, 100),
                bar(session_date, 15, 55, 100.0, 10_000),
            ]
        )
    return bars


def comparison_chart(
    calendar: XNYSCalendar,
    symbol: str,
    current_bars: list[YahooBar],
) -> YahooChart:
    previous = calendar.previous_trading_day(TRADING_DATE)
    return make_chart(symbol, [bar(previous, 15, 55, 100.0), *current_bars])


class ExchangeCalendarTest(unittest.TestCase):
    def test_xnys_handles_holiday_early_close_and_dst(self):
        calendar = XNYSCalendar()

        self.assertFalse(calendar.is_trading_day(date(2026, 7, 4)))
        self.assertEqual(
            calendar.regular_close(date(2026, 11, 27)).astimezone(NEW_YORK).hour,
            13,
        )
        self.assertEqual(calendar.regular_open(date(2026, 3, 6)).hour, 14)
        self.assertEqual(calendar.regular_open(date(2026, 3, 9)).hour, 13)


class InstrumentConfigTest(unittest.TestCase):
    def test_markdown_profile_controls_ticker_benchmark_and_peers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.md"
            path.write_text(
                "ticker: $AMD\nbenchmark: $SMH\npeer_tickers: $NVDA, $AVGO, $MRVL\n",
                encoding="utf-8",
            )

            profile = load_instrument_profile(path)

            self.assertEqual(profile.ticker, "AMD")
            self.assertEqual(profile.benchmark, "SMH")
            self.assertEqual(profile.peers, ("NVDA", "AVGO", "MRVL"))


class YahooMarketDataAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.calendar = XNYSCalendar()

    def test_reference_close_is_fixed_across_pre_regular_and_post(self):
        primary_bars = history_bars(self.calendar) + [
            bar(TRADING_DATE, 5, 0, 102.0),
            bar(TRADING_DATE, 10, 0, 104.0, 300),
            bar(TRADING_DATE, 17, 0, 105.0),
        ]
        charts = {"VRT": make_chart("VRT", primary_bars)}
        for symbol in ("SOXX", "ETN", "GEV", "NVT"):
            charts[symbol] = comparison_chart(
                self.calendar,
                symbol,
                [
                    bar(TRADING_DATE, 5, 0, 101.0),
                    bar(TRADING_DATE, 10, 0, 101.5),
                    bar(TRADING_DATE, 17, 0, 102.0),
                ],
            )
        adapter = YahooMarketDataAdapter(FakeChartClient(charts), self.calendar, PROFILE)

        pre = adapter.fetch_cycle(
            et(TRADING_DATE, 5, 1), last_observed_at=None
        ).frames[-1]
        regular = adapter.fetch_cycle(
            et(TRADING_DATE, 10, 1), last_observed_at=None
        ).frames[-1]
        post = adapter.fetch_cycle(
            et(TRADING_DATE, 17, 1), last_observed_at=None
        ).frames[-1]

        self.assertEqual(pre.reference_close, 100.0)
        self.assertEqual(regular.reference_close, 100.0)
        self.assertEqual(post.reference_close, 100.0)
        self.assertAlmostEqual(pre.snapshot.change_pct, 2.0)
        self.assertAlmostEqual(regular.snapshot.change_pct, 4.0)
        self.assertAlmostEqual(post.snapshot.change_pct, 5.0)

    def test_regular_quote_older_than_completed_bar_budget_is_rejected(self):
        charts = {
            "VRT": make_chart(
                "VRT",
                history_bars(self.calendar) + [bar(TRADING_DATE, 10, 0, 104.0, 100)],
            )
        }
        adapter = YahooMarketDataAdapter(FakeChartClient(charts), self.calendar, PROFILE)

        with self.assertRaises(StaleQuoteError):
            adapter.fetch_cycle(et(TRADING_DATE, 10, 11), last_observed_at=None)

    def test_same_time_volume_uses_only_matching_intraday_cutoff(self):
        primary = history_bars(self.calendar) + [
            bar(TRADING_DATE, 9, 30, 102.0, 200),
            bar(TRADING_DATE, 10, 0, 104.0, 200),
        ]
        charts = {"VRT": make_chart("VRT", primary)}
        for symbol, price in (("SOXX", 101.0), ("ETN", 102.0), ("GEV", 103.0)):
            charts[symbol] = comparison_chart(
                self.calendar,
                symbol,
                [bar(TRADING_DATE, 10, 0, price)],
            )
        adapter = YahooMarketDataAdapter(FakeChartClient(charts), self.calendar, PROFILE)

        cycle = adapter.fetch_cycle(et(TRADING_DATE, 10, 1), last_observed_at=None)

        self.assertEqual(cycle.volume.observed_volume, 400)
        self.assertEqual(cycle.volume.expected_volume, 200)
        self.assertEqual(cycle.volume.baseline_sessions, 20)
        self.assertEqual(cycle.volume.ratio, 2.0)
        snapshot = cycle.frames[-1].snapshot
        self.assertEqual(snapshot.peer_changes["NVT"], None)
        self.assertAlmostEqual(snapshot.peer_changes["ETN"], 2.0)

    def test_gap_replay_returns_only_bars_after_same_day_cursor(self):
        current = [
            bar(TRADING_DATE, 9, 30, 104.2, 100),
            bar(TRADING_DATE, 9, 35, 105.3, 100),
            bar(TRADING_DATE, 9, 40, 106.2, 100),
            bar(TRADING_DATE, 9, 45, 104.7, 100),
        ]
        charts = {"VRT": make_chart("VRT", history_bars(self.calendar) + current)}
        for symbol in ("SOXX", "ETN", "GEV", "NVT"):
            charts[symbol] = comparison_chart(
                self.calendar,
                symbol,
                [bar(TRADING_DATE, item.observed_at.astimezone(NEW_YORK).hour,
                     item.observed_at.astimezone(NEW_YORK).minute, 101.0)
                 for item in current],
            )
        adapter = YahooMarketDataAdapter(FakeChartClient(charts), self.calendar, PROFILE)

        cycle = adapter.fetch_cycle(
            et(TRADING_DATE, 9, 51),
            last_observed_at=et(TRADING_DATE, 9, 30),
        )

        self.assertEqual(len(cycle.frames), 3)
        self.assertEqual(cycle.replayed_frames, 2)
        self.assertEqual(cycle.frames[0].snapshot.observed_at, et(TRADING_DATE, 9, 35))

    def test_abnormal_move_requires_independent_quote_confirmation(self):
        primary = make_chart(
            "VRT",
            history_bars(self.calendar) + [bar(TRADING_DATE, 10, 0, 140.0, 100)],
        )
        quotes = {
            "VRT": YahooQuote(
                symbol="VRT",
                observed_at=et(TRADING_DATE, 10, 0),
                current_price=105.0,
                reference_close=100.0,
                session=MarketSession.REGULAR,
            )
        }
        adapter = YahooMarketDataAdapter(
            FakeChartClient({"VRT": primary}),
            self.calendar,
            PROFILE,
            quote_client=FakeQuoteClient(quotes),
        )

        with self.assertRaises(UnconfirmedQuoteError):
            adapter.fetch_cycle(et(TRADING_DATE, 10, 1), last_observed_at=None)

    def test_extended_quote_fallback_is_not_treated_as_relative_performance(self):
        primary = make_chart(
            "VRT",
            history_bars(self.calendar) + [bar(TRADING_DATE, 8, 20, 104.0)],
        )
        quotes = {
            symbol: YahooQuote(
                symbol=symbol,
                observed_at=et(TRADING_DATE - timedelta(days=1), 16, 0),
                current_price=100.0,
                reference_close=100.0,
                session=MarketSession.PRE,
                extended_fallback=True,
            )
            for symbol in ("SOXX", "ETN", "GEV", "NVT")
        }
        adapter = YahooMarketDataAdapter(
            FakeChartClient({"VRT": primary}),
            self.calendar,
            PROFILE,
            quote_client=FakeQuoteClient(quotes),
        )

        cycle = adapter.fetch_cycle(et(TRADING_DATE, 8, 21), last_observed_at=None)

        snapshot = cycle.frames[-1].snapshot
        self.assertIsNone(snapshot.benchmark_change_pct)
        self.assertTrue(all(value is None for value in snapshot.peer_changes.values()))


class MarketCycleServiceTest(unittest.TestCase):
    def test_replay_collapses_multiple_upward_bands_to_highest_event(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            frames = tuple(
                MarketFrame(
                    MarketSnapshot(
                        ticker="VRT",
                        trading_date=TRADING_DATE,
                        observed_at=et(TRADING_DATE, 10, minute),
                        session=MarketSession.REGULAR,
                        change_pct=change,
                        benchmark_change_pct=1.0,
                        peer_changes={"ETN": 1.0, "GEV": 1.2, "NVT": 1.1},
                    ),
                    close_price=100 + change,
                    reference_close=100,
                    cumulative_volume=100_000,
                )
                for minute, change in ((0, 4.2), (5, 5.3), (10, 6.2), (15, 4.7))
            )
            cycle = MarketCycle(
                ticker="VRT",
                trading_date=TRADING_DATE,
                frames=frames,
                volume=None,
                source_age_seconds=0,
            )

            report = MarketCycleService(repository).process(cycle)

            self.assertEqual(report.inserted_event_keys, ("VRT:2026-09-02:price-band:up:6",))
            self.assertEqual(repository.latest_market_observation_at("VRT"), frames[-1].snapshot.observed_at)
            pending = repository.pending_deliveries(
                datetime(2026, 9, 3, tzinfo=timezone.utc)
            )
            rendered = json.dumps(pending[0].payload, ensure_ascii=False)
            self.assertIn("+6.0% 상승 구간 진입", rendered)
            self.assertIn("정규장 · 09/02 23:10 KST", rendered)
            self.assertNotIn("104.7", rendered)
            self.assertNotIn("change_pct", json.dumps(report.as_dict()))
            self.assertEqual(report.latest_context["benchmark"]["outcome"], "outperform")
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                benchmark, peer_json = connection.execute(
                    "SELECT benchmark_change_pct, peer_changes_json "
                    "FROM market_observations ORDER BY observed_at DESC LIMIT 1"
                ).fetchone()
            self.assertEqual(benchmark, 1.0)
            self.assertEqual(json.loads(peer_json)["ETN"], 1.0)

    def test_volume_alert_is_once_per_day_and_combines_with_move(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = MarketCycleService(repository)

            combined = service.process(
                self._cycle(change=4.2, minute=0, observed=300, expected=200)
            )
            repeated = service.process(
                self._cycle(change=2.0, minute=5, observed=500, expected=200)
            )

            self.assertEqual(len(combined.inserted_event_keys), 1)
            self.assertIn("price-band", combined.inserted_event_keys[0])
            self.assertEqual(repeated.inserted_event_keys, ())
            pending = repository.pending_deliveries(
                datetime(2026, 9, 3, tzinfo=timezone.utc)
            )
            self.assertEqual(len(pending), 1)
            self.assertIn("거래량 동반", json.dumps(pending[0].payload, ensure_ascii=False))
            self.assertTrue(repository.load_price_band_state("VRT").volume_alerted)

    def test_volume_without_move_creates_one_independent_alert(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = MarketCycleService(repository)

            first = service.process(
                self._cycle(change=1.0, minute=0, observed=300, expected=200)
            )
            second = service.process(
                self._cycle(change=1.5, minute=5, observed=600, expected=200)
            )

            self.assertEqual(first.inserted_event_keys, ("VRT:2026-09-02:volume-spike",))
            self.assertEqual(second.inserted_event_keys, ())
            self.assertIn("거래량 1.5배 확대", json.dumps(first.messages[0], ensure_ascii=False))

    def test_shadow_records_alert_without_creating_delivery_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = MarketCycleService(repository, enqueue_alerts=False)

            report = service.process(
                self._cycle(change=4.2, minute=0, observed=100, expected=200)
            )

            self.assertEqual(len(report.inserted_event_keys), 1)
            self.assertEqual(
                repository.pending_deliveries(
                    datetime(2026, 9, 3, tzinfo=timezone.utc)
                ),
                [],
            )
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM alerts").fetchone()[0],
                    1,
                )

    def test_shadow_suppresses_deliveries_created_by_older_build(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            MarketCycleService(repository).process(
                self._cycle(change=4.2, minute=0, observed=100, expected=200)
            )

            suppressed = repository.suppress_pending_deliveries(
                datetime(2026, 9, 2, 15, tzinfo=timezone.utc),
                "shadow migration",
            )

            self.assertEqual(suppressed, 1)
            self.assertEqual(
                repository.pending_deliveries(
                    datetime(2026, 9, 3, tzinfo=timezone.utc)
                ),
                [],
            )
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                status, error = connection.execute(
                    "SELECT delivery_status, last_error FROM outbox"
                ).fetchone()
            self.assertEqual(status, "suppressed")
            self.assertEqual(error, "shadow migration")

    @staticmethod
    def _cycle(
        *,
        change: float,
        minute: int,
        observed: int,
        expected: int,
    ) -> MarketCycle:
        snapshot = MarketSnapshot(
            ticker="VRT",
            trading_date=TRADING_DATE,
            observed_at=et(TRADING_DATE, 10, minute),
            session=MarketSession.REGULAR,
            change_pct=change,
            benchmark_change_pct=0.5,
            peer_changes={"ETN": 0.4, "GEV": 0.6, "NVT": 0.5},
        )
        return MarketCycle(
            ticker="VRT",
            trading_date=TRADING_DATE,
            frames=(MarketFrame(snapshot, 100 + change, 100, observed),),
            volume=VolumeSnapshot(observed, expected, baseline_sessions=20),
            source_age_seconds=0,
        )


class YahooChartClientTest(unittest.TestCase):
    def test_429_switches_host_and_retries(self):
        class Response:
            def __init__(self, status_code, payload=None, headers=None):
                self.status_code = status_code
                self._payload = payload or {}
                self.headers = headers or {}

            def json(self):
                return self._payload

        payload = {
            "chart": {
                "error": None,
                "result": [
                    {
                        "timestamp": [int(et(TRADING_DATE, 10, 0).timestamp())],
                        "meta": {"symbol": "VRT"},
                        "indicators": {"quote": [{"close": [100.0], "volume": [10]}]},
                    }
                ],
            }
        }
        responses = iter(
            [
                Response(429, headers={"Retry-After": "7"}),
                Response(200, payload=payload),
            ]
        )
        urls: list[str] = []
        sleeps: list[float] = []

        def get(url, **_kwargs):
            urls.append(url)
            return next(responses)

        client = YahooChartClient(
            get=get,
            sleeper=sleeps.append,
            retry_delays=(0, 15),
        )

        chart = client.fetch("VRT", interval="5m", range_="5d", include_prepost=True)

        self.assertEqual(chart.symbol, "VRT")
        self.assertIn("query1", urls[0])
        self.assertIn("query2", urls[1])
        self.assertEqual(sleeps, [7.0])

    def test_spark_parses_multiple_symbols_in_one_request(self):
        class Response:
            status_code = 200
            headers = {}

            def json(self):
                response = {
                    "timestamp": [int(et(TRADING_DATE, 10, 0).timestamp())],
                    "meta": {},
                    "indicators": {"quote": [{"close": [100.0], "volume": [10]}]},
                }
                return {
                    "spark": {
                        "result": [
                            {"symbol": "VRT", "response": [response]},
                            {"symbol": "SOXX", "response": [response]},
                        ]
                    }
                }

        urls: list[str] = []

        def get(url, **_kwargs):
            urls.append(url)
            return Response()

        client = YahooChartClient(get=get, retry_delays=(0,))

        charts = client.fetch_many(
            ("VRT", "SOXX"),
            interval="5m",
            range_="1mo",
            include_prepost=True,
        )

        self.assertEqual(set(charts), {"VRT", "SOXX"})
        self.assertEqual(len(urls), 1)
        self.assertIn("/spark", urls[0])

    def test_pre_market_quote_uses_previous_regular_close_as_reference(self):
        observed_at = et(TRADING_DATE, 8, 20)
        payload = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "VRT",
                        "regularMarketPrice": 255.97,
                        "regularMarketPreviousClose": 258.72,
                        "regularMarketTime": int(et(TRADING_DATE - timedelta(days=1), 16, 0).timestamp()),
                        "preMarketPrice": 257.0,
                        "preMarketTime": int(observed_at.timestamp()),
                    }
                ]
            }
        }

        quote = parse_quote_payload(payload, session=MarketSession.PRE)["VRT"]

        self.assertEqual(quote.reference_close, 255.97)
        self.assertEqual(quote.current_price, 257.0)
        self.assertEqual(quote.observed_at, observed_at)

    def test_pre_market_quote_marks_regular_price_fallback(self):
        payload = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "ETN",
                        "regularMarketPrice": 350.0,
                        "regularMarketPreviousClose": 345.0,
                        "regularMarketTime": int(
                            et(TRADING_DATE - timedelta(days=1), 16, 0).timestamp()
                        ),
                    }
                ]
            }
        }

        quote = parse_quote_payload(payload, session=MarketSession.PRE)["ETN"]

        self.assertTrue(quote.extended_fallback)
        self.assertEqual(quote.change_pct, 0.0)


if __name__ == "__main__":
    unittest.main()
