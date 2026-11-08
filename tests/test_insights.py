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
sys.path.insert(0, str(ROOT / "tests"))

from investing_monitor.adapters.config import load_insight_profile
from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION, SQLiteMonitorRepository
from investing_monitor.application.briefs import CloseBriefService
from investing_monitor.application.insights import InsightService, QUALITY_ALERT_TYPE
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.evidence import EvidenceProfile
from investing_monitor.domain.insights import (
    ConvertibleNote,
    InsightBundle,
    InsightProfile,
    OptionQuote,
    convertible_watch,
    earnings_quality,
    options_insight,
    rank_insight_lines,
    residual_drift,
    short_interest_insight,
)
from investing_monitor.domain.models import InstrumentProfile, MarketSensitivity, Position
from investing_monitor.presentation.insight_messages import close_insight_text
from investing_monitor.presentation.quality import FORBIDDEN_TEXT, audit_message

from test_close_brief import CREATED_AT, OPEN_AT, TRADING_DATE, market_cycle, visible_text


def _closes(values: list[float], end: date = date(2026, 11, 6)) -> dict[date, float]:
    out, day = {}, end
    for value in reversed(values):
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        out[day] = value
        day -= timedelta(days=1)
    return out


def _series(start: float, daily_pct: list[float]) -> list[float]:
    values = [start]
    for pct in daily_pct:
        values.append(values[-1] * (1 + pct / 100))
    return values


SENSITIVITY = MarketSensitivity(
    ticker="BE",
    benchmark_symbol="SOXX",
    peer_symbols=("VRT", "GEV"),
    calculated_at=datetime(2026, 11, 1, tzinfo=timezone.utc),
    benchmark_beta=1.1,
    benchmark_residual_band_pct=3.0,
    benchmark_samples=120,
    peer_beta=1.25,
    peer_residual_band_pct=2.0,
    peer_samples=120,
)


class ResidualDriftTest(unittest.TestCase):
    def test_splits_today_into_peer_linked_and_idiosyncratic_parts(self):
        peer_moves = [0.5, -0.5] * 15 + [8.0]  # peers +8% on the last day
        own_moves = [0.6, -0.6] * 15 + [3.0]   # the name only +3%
        closes = {
            "BE": _closes(_series(100, own_moves)),
            "VRT": _closes(_series(50, peer_moves)),
            "GEV": _closes(_series(500, peer_moves)),
            "SOXX": _closes(_series(200, peer_moves)),
        }
        drift = residual_drift(
            ticker="BE",
            daily_closes=closes,
            sensitivity=SENSITIVITY,
            benchmark_symbol="SOXX",
            peer_symbols=("VRT", "GEV"),
        )
        self.assertIsNotNone(drift)
        self.assertEqual(drift.basis, "peer")
        self.assertAlmostEqual(drift.actual_pct, 3.0, places=1)
        self.assertAlmostEqual(drift.expected_pct, 10.0, places=1)
        self.assertAlmostEqual(drift.residual_pct, -7.0, places=1)
        self.assertLessEqual(drift.residual_z, -3.0)
        self.assertIn("고유", drift.label)

    def test_without_a_model_there_is_no_drift(self):
        self.assertIsNone(
            residual_drift(
                ticker="BE",
                daily_closes={"BE": _closes(_series(100, [1.0] * 30))},
                sensitivity=None,
                benchmark_symbol="SOXX",
                peer_symbols=(),
            )
        )


def _chain(spot: float, as_of: date) -> list[OptionQuote]:
    quotes = []
    for days, iv in ((12, 1.10), (40, 0.95), (68, 0.90), (103, 0.89)):
        expiration = as_of + timedelta(days=days)
        for strike in (spot * 0.8, spot * 0.9, spot, spot * 1.1, spot * 1.2):
            premium = spot * iv * (days / 365) ** 0.5 * 0.4
            for option_type, oi in (("call", 1000 if strike > spot else 300), ("put", 1500 if strike < spot else 300)):
                quotes.append(
                    OptionQuote(
                        expiration=expiration,
                        strike=round(strike, 2),
                        option_type=option_type,
                        open_interest=oi,
                        volume=100,
                        bid=premium * 0.98,
                        ask=premium * 1.02,
                        last=premium,
                        implied_volatility=iv,
                    )
                )
    return quotes


class OptionsInsightTest(unittest.TestCase):
    def test_summarises_open_interest_term_structure_and_event_move(self):
        as_of = date(2026, 11, 6)
        insight = options_insight(
            _chain(175.0, as_of),
            spot=175.0,
            as_of=as_of,
            next_earnings=as_of + timedelta(days=90),
        )
        self.assertIsNotNone(insight)
        self.assertGreater(insight.put_call_open_interest, 1.0)
        self.assertEqual(insight.expiry_30d, as_of + timedelta(days=40))
        self.assertAlmostEqual(insight.iv30_pct, 95.0)
        self.assertEqual(insight.earnings_expiry, as_of + timedelta(days=103))
        self.assertIsNotNone(insight.earnings_move_pct)
        self.assertIsNotNone(insight.event_move_pct)
        self.assertTrue(all(strike > 175 for strike, _ in insight.call_walls))
        self.assertTrue(all(strike < 175 for strike, _ in insight.put_walls))

    def test_empty_chain_returns_nothing(self):
        self.assertIsNone(options_insight([], spot=100.0, as_of=date(2026, 11, 6)))


class ConvertibleAndShortInterestTest(unittest.TestCase):
    def test_convertible_watch_counts_days_above_the_trigger(self):
        note = ConvertibleNote(label="0% 전환사채", conversion_price=100.0, trigger_multiple=1.3, max_shares_millions=19.5)
        closes = _closes([120.0] * 10 + [135.0] * 20)
        watch = convertible_watch(note, closes)
        self.assertEqual(watch.trigger_price, 130.0)
        self.assertEqual(watch.days_above_trigger, 20)
        self.assertTrue(watch.in_the_money)
        self.assertAlmostEqual(watch.distance_to_conversion_pct, 35.0)

    def test_short_interest_from_quote_summary_fields(self):
        insight = short_interest_insight(
            {
                "sharesShort": 17_191_709,
                "sharesShortPriorMonth": 20_982_242,
                "shortPercentOfFloat": 0.0592,
                "dateShortInterest": 1760486400,  # 2025-10-15 UTC
                "shortRatio": 1.33,
            }
        )
        self.assertEqual(insight.shares_short, 17_191_709)
        self.assertAlmostEqual(insight.change_pct, -18.1, places=1)
        self.assertAlmostEqual(insight.percent_of_float, 5.92)
        self.assertEqual(insight.report_date.month, 10)
        self.assertIsNone(short_interest_insight({}))


def _facts() -> dict:
    def flow(tag, values):
        rows = []
        for (start, end, val, form) in values:
            rows.append({"start": start, "end": end, "val": val, "form": form, "filed": end, "accn": "0001628280-26-066931", "fp": "Q3"})
        return {tag: {"units": {"USD": rows}}}

    def stock(tag, values):
        return {tag: {"units": {"USD": [{"end": end, "val": val, "form": "10-Q", "filed": end, "accn": "0001628280-26-066931"} for end, val in values]}}}

    facts = {}
    facts.update(flow("Revenues", [
        ("2026-01-01", "2026-03-31", 751e6, "10-Q"),
        ("2026-01-01", "2026-06-30", 1816e6, "10-Q"),   # YTD only → derived Q2 = 1065
        ("2026-04-01", "2026-06-30", 1065e6, "10-Q"),
        ("2026-07-01", "2026-09-30", 1236e6, "10-Q"),
        ("2026-01-01", "2026-09-30", 3052e6, "10-Q"),
    ]))
    facts.update(flow("NetCashProvidedByUsedInOperatingActivities", [
        ("2026-01-01", "2026-03-31", 74e6, "10-Q"),
        ("2026-01-01", "2026-06-30", 300e6, "10-Q"),
        ("2026-01-01", "2026-09-30", 273e6, "10-Q"),   # Q3 derived = -27
    ]))
    facts.update(flow("NetIncomeLoss", [
        ("2026-01-01", "2026-03-31", 71e6, "10-Q"),
        ("2026-04-01", "2026-06-30", 196e6, "10-Q"),
        ("2026-07-01", "2026-09-30", 250e6, "10-Q"),
    ]))
    facts.update(flow("PaymentsToAcquirePropertyPlantAndEquipment", [
        ("2026-01-01", "2026-03-31", 26e6, "10-Q"),
        ("2026-01-01", "2026-06-30", 78e6, "10-Q"),
        ("2026-01-01", "2026-09-30", 231e6, "10-Q"),
    ]))
    facts.update(stock("ContractWithCustomerAssetNetCurrent", [("2026-03-31", 243e6), ("2026-06-30", 365e6), ("2026-09-30", 793e6)]))
    facts.update(stock("AccountsReceivableNetCurrent", [("2026-03-31", 359e6), ("2026-06-30", 458e6), ("2026-09-30", 635e6)]))
    facts.update(stock("InventoryNet", [("2026-03-31", 733e6), ("2026-06-30", 758e6), ("2026-09-30", 928e6)]))
    facts.update(stock("AccountsPayableCurrent", [("2026-03-31", 242e6), ("2026-06-30", 310e6), ("2026-09-30", 493e6)]))
    return facts


class EarningsQualityTest(unittest.TestCase):
    def test_derives_quarters_from_mixed_ytd_facts_and_flags_cash_conversion(self):
        quality = earnings_quality(_facts(), cik="0001664703")
        self.assertIsNotNone(quality)
        latest = quality.latest
        self.assertEqual(latest.period_end, date(2026, 9, 30))
        self.assertAlmostEqual(latest.values["revenue"], 1236.0)
        self.assertAlmostEqual(latest.values["operating_cash_flow"], -27.0)
        self.assertAlmostEqual(quality.quarters[-2].values["operating_cash_flow"], 226.0)
        self.assertAlmostEqual(latest.values["contract_assets"], 793.0)
        rendered = "\n".join(quality.verdict)
        self.assertIn("계약자산", rendered)
        self.assertIn("+428M", rendered)
        self.assertIn("영업현금흐름 $-27M", rendered)
        self.assertIn("매입채무", rendered)
        self.assertTrue(quality.source_url.startswith("https://www.sec.gov/Archives/edgar/data/1664703/"))


class FakeMarket:
    def __init__(self, closes, quotes=None, short_fields=None, earnings=None, fail=()):
        self.closes, self.quotes, self.short_fields, self.earnings, self.fail = closes, quotes or [], short_fields or {}, earnings, set(fail)

    def fetch_daily_closes(self, symbols):
        if "closes" in self.fail:
            raise RuntimeError("yahoo down")
        return {s: self.closes[s] for s in symbols if s in self.closes}

    def fetch_option_quotes(self, ticker, *, max_expiries=8):
        if "options" in self.fail:
            raise RuntimeError("chain down")
        return tuple(self.quotes)

    def fetch_short_interest_fields(self, ticker):
        return dict(self.short_fields)

    def fetch_next_earnings(self, ticker, *, today):
        return self.earnings


class FakeFacts:
    def __init__(self, facts):
        self.facts = facts

    def fetch(self, cik, *, contact):
        return self.facts


PROFILE = InstrumentProfile(ticker="BE", benchmark="SOXX", peers=("VRT", "GEV"))
EVIDENCE = EvidenceProfile(ticker="BE", company_name="Bloom Energy", cik="0001664703", aliases=("Bloom Energy",), sec_contact="a@b.c")
INSIGHT_PROFILE = InsightProfile(
    customer_proxies=(("ORCL", "최대 고객 프록시"),),
    convertibles=(ConvertibleNote(label="0% 전환사채 2030", conversion_price=194.97, max_shares_millions=19.55),),
)


def _market(as_of=date(2026, 11, 6)) -> FakeMarket:
    peer_moves = [0.5, -0.5] * 15 + [8.0]
    own_moves = [0.6, -0.6] * 15 + [3.0]
    orcl_moves = [-2.0] * 5 + [0.0] * 25 + [-1.0]
    closes = {
        "BE": _closes(_series(168.0, own_moves), as_of),
        "VRT": _closes(_series(50, peer_moves), as_of),
        "GEV": _closes(_series(500, peer_moves), as_of),
        "SOXX": _closes(_series(200, peer_moves), as_of),
        "ORCL": _closes(_series(230, orcl_moves), as_of),
    }
    spot = closes["BE"][max(closes["BE"])]
    return FakeMarket(
        closes,
        quotes=_chain(spot, as_of),
        short_fields={"sharesShort": 17_191_709, "sharesShortPriorMonth": 20_982_242, "shortPercentOfFloat": 0.0592, "dateShortInterest": 1760486400, "shortRatio": 1.33},
        earnings=as_of + timedelta(days=90),
    )


class InsightServiceTest(unittest.TestCase):
    def test_close_report_ranks_lines_persists_snapshot_and_avoids_forbidden_words(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = InsightService(
                repository, _market(), FakeFacts(_facts()),
                profile=PROFILE, evidence_profile=EVIDENCE, insight_profile=INSIGHT_PROFILE,
                position=Position(average_price=150.0, shares=100),
            )
            report = service.close_report(date(2026, 11, 6), sensitivity=SENSITIVITY)
            kinds = [line.kind for line in report.lines]
            self.assertEqual(kinds[0], "drift")  # the unexplained move leads
            self.assertIn("short_interest", kinds)  # first sighting of the report
            self.assertIn("iv", kinds)
            rendered = close_insight_text(report.lines)
            self.assertIn("섹터로 설명되지 않는 움직임", rendered)
            for forbidden in FORBIDDEN_TEXT:
                self.assertNotIn(forbidden.lower(), rendered.lower())
            self.assertEqual(report.errors, ())
            snapshot = repository.load_insight_snapshot("BE")
            self.assertEqual(snapshot["trading_date"], "2026-11-06")
            self.assertIsNotNone(snapshot["iv30_pct"])

            # next session: the same short-interest report is not news any more
            service.market = _market(date(2026, 11, 9))
            later = service.close_report(date(2026, 11, 9), sensitivity=SENSITIVITY)
            self.assertNotIn("short_interest", [line.kind for line in later.lines])

    def test_partial_provider_failure_degrades_instead_of_dropping_the_brief(self):
        market = _market()
        market.fail = {"options"}
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = InsightService(repository, market, FakeFacts({}), profile=PROFILE, evidence_profile=EVIDENCE, insight_profile=INSIGHT_PROFILE)
            report = service.close_report(date(2026, 11, 6), sensitivity=SENSITIVITY)
            self.assertTrue(report.lines)
            self.assertTrue(any("options" in error for error in report.errors))
            self.assertIsNone(report.bundle.options)

    def test_quarterly_checkpoint_alerts_once_with_a_source_link(self):
        now = datetime(2026, 11, 8, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = InsightService(repository, _market(), FakeFacts(_facts()), profile=PROFILE, evidence_profile=EVIDENCE, insight_profile=INSIGHT_PROFILE)
            first = service.quarterly_checkpoint(now)
            second = service.quarterly_checkpoint(now)
            self.assertTrue(first.inserted)
            self.assertFalse(second.inserted)
            self.assertEqual(second.skipped, "already alerted")
            self.assertEqual(first.event_key, f"BE:2026-09-30:{QUALITY_ALERT_TYPE}")
            with closing(sqlite3.connect(repository.path)) as connection:
                row = connection.execute("SELECT alert_type, payload_json FROM alerts").fetchone()
            payload = json.loads(row[1])
            self.assertEqual(row[0], QUALITY_ALERT_TYPE)
            self.assertTrue(audit_message(QUALITY_ALERT_TYPE, payload).passed)
            text = visible_text(payload)
            self.assertIn("이익의 질 체크포인트", text)
            self.assertIn("계약자산(미청구 매출) $793M", text)
            self.assertIn("SEC 원문 보기", text)

    def test_stale_quarter_is_not_alerted(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = InsightService(repository, _market(), FakeFacts(_facts()), profile=PROFILE, evidence_profile=EVIDENCE, insight_profile=INSIGHT_PROFILE)
            report = service.quarterly_checkpoint(datetime(2027, 6, 1, tzinfo=timezone.utc))
            self.assertFalse(report.inserted)
            self.assertIn("older", report.skipped)


class CloseBriefWithInsightsTest(unittest.TestCase):
    def test_insight_section_sits_between_volume_and_levels_and_passes_the_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            MarketCycleService(repository, enqueue_alerts=False).process(market_cycle())
            market = _market(TRADING_DATE)
            market.closes = {**market.closes, "VRT": market.closes["BE"]}
            insight = InsightService(
                repository, _market(TRADING_DATE), FakeFacts({}),
                profile=InstrumentProfile(ticker="BE", benchmark="SOXX", peers=("VRT", "GEV")),
                evidence_profile=EVIDENCE, insight_profile=INSIGHT_PROFILE,
            ).close_report(TRADING_DATE, sensitivity=SENSITIVITY)
            report = CloseBriefService(repository, enqueue_alerts=False).process(
                "VRT", TRADING_DATE, trading_open_at=OPEN_AT, created_at=CREATED_AT, insights=insight.lines,
            )
            rendered = json.dumps(report.payload, ensure_ascii=False)
            self.assertLess(rendered.index("거래량 평시 수준"), rendered.index("오늘 평소와 다른 점"))
            self.assertTrue(audit_message("daily_close", report.payload).passed)
            for forbidden in ("DCA", "RSI", "MACD", "PCR", "FINRA"):
                self.assertNotIn(forbidden, rendered)


class RepositoryAndConfigTest(unittest.TestCase):
    def test_schema_version_bumped_for_insight_snapshots(self):
        self.assertEqual(SCHEMA_VERSION, 14)
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            repository.save_insight_snapshot("be", date(2026, 11, 5), {"iv30_pct": 90.0})
            repository.save_insight_snapshot("be", date(2026, 11, 6), {"iv30_pct": 95.0})
            self.assertEqual(repository.load_insight_snapshot("BE")["iv30_pct"], 95.0)
            self.assertEqual(repository.load_insight_snapshot("BE", before=date(2026, 11, 6))["iv30_pct"], 90.0)
            self.assertIsNone(repository.load_insight_snapshot("BE", before=date(2026, 11, 5)))

    def test_insight_profile_is_parsed_from_monitor_config(self):
        profile = load_insight_profile(ROOT / "monitor_config.md")
        self.assertEqual(profile.customer_proxies[0][0], "ORCL")
        self.assertEqual(profile.convertibles[0].conversion_price, 194.97)
        self.assertEqual(profile.convertibles[0].max_shares_millions, 19.55)


if __name__ == "__main__":
    unittest.main()
