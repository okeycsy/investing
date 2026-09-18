from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import (
    MarketCycle,
    MarketFrame,
    MarketSensitivity,
    MarketSession,
    MarketSnapshot,
    RelativeOutcome,
    RelativeStrength,
    SituationVerdict,
    VolumeSignal,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    PriceBandPolicy,
    assess_intraday_volume,
    assess_market_situation,
    assess_relative_performance,
)
from investing_monitor.domain.situation import compare_market_context
from investing_monitor.presentation.close_messages import build_close_message
from investing_monitor.presentation.market_context import delta_text
from investing_monitor.presentation.quality import audit_message
from investing_monitor.presentation.slack_messages import (
    build_price_band_message,
    build_volume_message,
)
from investing_monitor.presentation.weekly_messages import build_weekly_message


AT = datetime(2026, 9, 17, 15, 0, tzinfo=timezone.utc)
DAY = AT.date()


def market(change: float, comparison: float = 1.0, **overrides) -> MarketSnapshot:
    values = {
        "ticker": "VRT",
        "trading_date": DAY,
        "observed_at": AT,
        "session": MarketSession.REGULAR,
        "change_pct": change,
        "benchmark_change_pct": comparison,
        "peer_changes": {"ETN": comparison - 0.2, "GEV": comparison + 0.2, "NVT": None},
    }
    values.update(overrides)
    return MarketSnapshot(**values)


class RelativeStrengthPolicyTest(unittest.TestCase):
    def test_all_boundaries_are_inclusive_symmetric_and_use_return_differences(self):
        cases = (
            (0.0, None),
            (0.29999, None),
            (0.3, None),
            (0.30001, RelativeStrength.SLIGHT),
            (0.79999, RelativeStrength.SLIGHT),
            (0.8, RelativeStrength.SLIGHT),
            (0.80001, RelativeStrength.SIGNIFICANT),
            (1.99999, RelativeStrength.SIGNIFICANT),
            (2.0, RelativeStrength.SIGNIFICANT),
            (2.00001, RelativeStrength.STRONG),
        )
        for comparison in (-3.0, 1.0, 4.0):
            for sign in (-1, 1):
                for gap, strength in cases:
                    with self.subTest(comparison=comparison, sign=sign, gap=gap):
                        relative = assess_relative_performance(
                            market(comparison + sign * gap, comparison)
                        )
                        outcome = (
                            RelativeOutcome.INLINE if strength is None
                            else RelativeOutcome.OUTPERFORM if sign > 0
                            else RelativeOutcome.UNDERPERFORM
                        )
                        self.assertEqual(relative.benchmark, outcome)
                        self.assertEqual(relative.peers, outcome)
                        self.assertEqual(relative.benchmark_strength, strength)
                        self.assertEqual(relative.peer_strength, strength)
                        self.assertEqual(relative.peer_symbols, ("ETN", "GEV"))

    def test_insufficient_comparisons_are_unavailable_not_neutral(self):
        relative = assess_relative_performance(market(
            4.0, benchmark_change_pct=None, peer_changes={"ETN": 0.0, "GEV": None},
        ))
        self.assertEqual(relative.benchmark, RelativeOutcome.UNAVAILABLE)
        self.assertEqual(relative.peers, RelativeOutcome.UNAVAILABLE)
        self.assertIsNone(relative.benchmark_strength)
        self.assertIsNone(relative.peer_strength)

    def test_beta_model_does_not_replace_actual_relative_performance(self):
        snapshot = market(4.4, 3.0)
        model = MarketSensitivity(
            ticker="VRT", benchmark_symbol="SOXX", peer_symbols=("ETN", "GEV", "NVT"),
            calculated_at=AT, benchmark_beta=1.5, benchmark_residual_band_pct=1.0,
            benchmark_samples=80, peer_beta=1.5, peer_residual_band_pct=1.0, peer_samples=80,
        )
        relative = assess_relative_performance(snapshot, sensitivity=model)
        situation = assess_market_situation(snapshot, relative)
        self.assertEqual(relative.benchmark, RelativeOutcome.OUTPERFORM)
        self.assertEqual(relative.benchmark_strength, RelativeStrength.SIGNIFICANT)
        self.assertEqual(relative.peers, RelativeOutcome.OUTPERFORM)
        self.assertEqual(relative.peer_strength, RelativeStrength.SIGNIFICANT)
        self.assertEqual(relative.benchmark_expected, RelativeOutcome.INLINE)
        self.assertEqual(relative.peers_expected, RelativeOutcome.INLINE)
        self.assertEqual(situation.verdict, SituationVerdict.BROADLY_EXPLAINED)
        payload = build_close_message(snapshot, relative, None, assess_intraday_volume(None), (), situation)
        text = json.dumps(payload, ensure_ascii=False)
        self.assertIn("상당한 아웃퍼폼", text)
        self.assertIn("민감도 참고", text)
        self.assertLess(text.index("상당한 아웃퍼폼"), text.index("민감도 참고"))
        self.assertNotIn("시장·피어 흐름으로 설명되는", text)


class RelativeMessageTest(unittest.TestCase):
    def test_every_market_message_renders_identical_symmetric_grades(self):
        volume = VolumeSnapshot(200, 100, baseline_sessions=20)
        volume_assessment = assess_intraday_volume(volume)
        cases = ((0.3, "비슷한 흐름"), (0.8, "약간"), (2.0, "상당한"), (2.01, "강한"))
        for sign in (-1, 1):
            for gap, label in cases:
                with self.subTest(sign=sign, gap=gap):
                    snapshot = market(sign * 4.73, sign * (4.73 - gap))
                    relative = assess_relative_performance(snapshot)
                    signal, _ = PriceBandPolicy().evaluate(snapshot, None)
                    payloads = {
                        "price_band": build_price_band_message(signal, relative, volume, volume_assessment, ()),
                        "volume_spike": build_volume_message(
                            VolumeSignal("volume", "VRT", DAY, AT), snapshot, relative, volume, volume_assessment,
                        ),
                        "daily_close": build_close_message(snapshot, relative, volume, volume_assessment, ()),
                        "weekly_review": build_weekly_message(
                            snapshot, relative, volume, volume_assessment, (), (), (),
                            period_start=date(2026, 9, 14), period_end=date(2026, 9, 18), session_count=5,
                        ),
                    }
                    outcome_label = (
                        label if gap == 0.3
                        else f"{label} {'아웃퍼폼' if sign > 0 else '언더퍼폼'}"
                    )
                    for kind, payload in payloads.items():
                        text = json.dumps(payload, ensure_ascii=False)
                        self.assertIn(f"반도체 지수(SOXX) 대비 {outcome_label}", text)
                        self.assertIn(f"대비 {outcome_label}", text)
                        self.assertEqual(text.count(outcome_label), 2)
                        self.assertNotIn("4.73", text)
                        self.assertNotIn("%p", text)
                        self.assertTrue(audit_message(kind, payload).passed)

    def test_degree_changes_survive_restart_and_do_not_add_alert_triggers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            for index, change in enumerate((4.4, 5.2, 6.2)):
                repository = SQLiteMonitorRepository(path)
                snapshot = market(change, 4.0, observed_at=AT + timedelta(minutes=5 * index))
                cycle = MarketCycle(
                    ticker="VRT", trading_date=DAY,
                    frames=(MarketFrame(snapshot, 100 + change, 100),),
                    volume=None, source_age_seconds=0,
                )
                report = MarketCycleService(repository, enqueue_alerts=False).process(cycle)
                self.assertEqual(len(report.inserted_event_keys), 1)
                if index:
                    text = json.dumps(report.messages[0], ensure_ascii=False)
                    transition = (
                        "약간 아웃퍼폼 → 상당한 아웃퍼폼" if index == 1
                        else "상당한 아웃퍼폼 → 강한 아웃퍼폼"
                    )
                    self.assertIn(transition, text)
                latest = repository.latest_price_alert_context("VRT", DAY, "up")
                self.assertEqual(latest.context["version"], 2)
                self.assertEqual(latest.context["relative_basis"], "raw_return_gap_v1")
            self.assertEqual(latest.context["benchmark_strength"], "strong")
            duplicate = MarketCycleService(repository, enqueue_alerts=False).process(cycle)
            self.assertEqual(duplicate.inserted_event_keys, ())
            self.assertEqual(repository.pending_deliveries(AT + timedelta(days=1)), [])

    def test_legacy_adjusted_outcomes_do_not_appear_as_real_market_changes(self):
        previous = {"benchmark_outcome": "inline", "peer_outcome": "inline"}
        current = {
            "relative_basis": "raw_return_gap_v1",
            "benchmark_outcome": "outperform", "benchmark_strength": "strong",
            "peer_outcome": "underperform", "peer_strength": "slight",
        }
        delta = compare_market_context(previous, current)
        self.assertIsNone(delta.benchmark)
        self.assertIsNone(delta.peers)
        self.assertNotIn("아웃퍼폼", delta_text(delta))


if __name__ == "__main__":
    unittest.main()
