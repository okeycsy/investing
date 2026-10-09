from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.domain.levels import (
    DailyBar,
    PriceLevels,
    compute_price_levels,
)
from investing_monitor.domain.models import Position
from investing_monitor.domain.policies import RapidMovePolicy
from investing_monitor.domain.models import (
    Direction,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    PriceBandState,
)
from investing_monitor.presentation.levels import (
    day_level_review,
    levels_text,
    position_text,
)

TODAY = date(2026, 10, 9)
AT = datetime(2026, 10, 9, 18, 0, tzinfo=timezone.utc)


def history(closes, *, spread=1.0):
    days = []
    day = TODAY - timedelta(days=1)
    while len(days) < len(closes):
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days.reverse()
    return [
        DailyBar(
            trading_date=trading_date,
            close=close,
            high=close + spread,
            low=close - spread,
        )
        for trading_date, close in zip(days, closes)
    ]


class ComputePriceLevelsTest(unittest.TestCase):
    def test_moving_averages_atr_and_52w_range(self):
        closes = [100.0 + index * 0.5 for index in range(220)]
        levels = compute_price_levels(
            "vrt",
            history(closes),
            trading_date=TODAY,
            computed_at=AT,
        )

        self.assertEqual(levels.ticker, "VRT")
        self.assertAlmostEqual(levels.last_close, closes[-1])
        self.assertAlmostEqual(levels.sma20, sum(closes[-20:]) / 20)
        self.assertAlmostEqual(levels.sma50, sum(closes[-50:]) / 50)
        self.assertAlmostEqual(levels.sma200, sum(closes[-200:]) / 200)
        self.assertAlmostEqual(levels.high_52w, closes[-1] + 1.0)
        self.assertAlmostEqual(levels.low_52w, min(closes[-252:]) - 1.0)
        # Each bar spans high-low = 2.0, which dominates the true range.
        self.assertAlmostEqual(levels.atr14, 2.0, delta=0.01)

    def test_triple_tested_floor_becomes_the_nearest_support(self):
        closes = [250.0] * 57 + [246.0, 244.0, 244.5]
        bars = history(closes, spread=0.5)
        # Three sessions probe the same floor near 240 (like VRT 10/07-10/09).
        for index in (-3, -2, -1):
            bar = bars[index]
            bars[index] = DailyBar(
                trading_date=bar.trading_date,
                close=bar.close,
                high=bar.close + 0.5,
                low=239.9,
            )
        levels = compute_price_levels(
            "VRT",
            bars,
            trading_date=TODAY,
            computed_at=AT,
        )

        self.assertTrue(levels.supports)
        nearest = levels.supports[0]
        self.assertAlmostEqual(nearest.price, 239.9, delta=0.2)
        self.assertGreaterEqual(nearest.touches, 3)

    def test_insufficient_history_degrades_without_crashing(self):
        levels = compute_price_levels(
            "VRT",
            history([100.0, 101.0, 102.0]),
            trading_date=TODAY,
            computed_at=AT,
        )

        self.assertIsNone(levels.sma20)
        self.assertIsNone(levels.atr14)
        self.assertEqual(levels.last_close, 102.0)

    def test_repository_round_trip(self):
        levels = compute_price_levels(
            "VRT",
            history([100.0 + index for index in range(60)]),
            trading_date=TODAY,
            computed_at=AT,
        )
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            repository.save_price_levels(levels)

            loaded = repository.load_price_levels("vrt", TODAY)

            self.assertEqual(loaded, levels)
            self.assertIsNone(repository.load_price_levels("vrt", TODAY - timedelta(days=1)))


class LevelsPresentationTest(unittest.TestCase):
    def test_levels_text_shows_supports_averages_and_yearly_range(self):
        closes = [250.0] * 57 + [246.0, 244.0, 244.5]
        bars = history(closes, spread=0.5)
        levels = compute_price_levels("VRT", bars, trading_date=TODAY, computed_at=AT)

        text = levels_text(levels, 244.49)

        self.assertIn("주요 레벨", text)
        self.assertIn("SMA20", text)
        self.assertIn("52주", text)
        self.assertIn("고점 대비", text)

    def test_position_text_reports_unrealized_return(self):
        text = position_text(Position(average_price=355.0, shares=100), 244.49)

        self.assertIn("평단 $355.00", text)
        self.assertIn("-31.13%", text)
        self.assertIn("100주", text)
        self.assertIn("-$11,051", text)

    def test_position_without_shares_keeps_only_percentage(self):
        text = position_text(Position(average_price=355.0), 244.49)

        self.assertIn("-31.13%", text)
        self.assertNotIn("주 ·", text)

    def test_day_level_review_marks_defended_support_and_sma_cross(self):
        closes = [250.0] * 57 + [246.0, 244.0, 244.5]
        bars = history(closes, spread=0.5)
        for index in (-3, -2, -1):
            bar = bars[index]
            bars[index] = DailyBar(
                trading_date=bar.trading_date,
                close=bar.close,
                high=bar.close + 0.5,
                low=239.9,
            )
        levels = compute_price_levels("VRT", bars, trading_date=TODAY, computed_at=AT)

        held = day_level_review(levels, 244.4, 239.95, 248.0)
        broken = day_level_review(levels, 236.0, 235.5, 245.0)

        self.assertIn("오늘의 레벨 리뷰", held)
        self.assertIn("테스트 후 사수", held)
        self.assertIn("이탈 마감", broken)

    def test_day_level_review_silent_when_no_level_was_touched(self):
        from investing_monitor.domain.levels import ClusteredLevel

        levels = PriceLevels(
            ticker="VRT",
            trading_date=TODAY,
            computed_at=AT,
            atr14=2.0,
            supports=(ClusteredLevel(230.0, 3),),
            resistances=(ClusteredLevel(260.0, 4),),
            last_close=250.0,
        )

        self.assertEqual(day_level_review(levels, 250.1, 249.8, 250.4), "")


def frame(change_15m, ratio_15m, *, minute=0):
    observed = AT + timedelta(minutes=minute)
    snapshot = MarketSnapshot(
        ticker="VRT",
        trading_date=TODAY,
        observed_at=observed,
        session=MarketSession.REGULAR,
        change_pct=-2.0,
    )
    return MarketFrame(
        snapshot=snapshot,
        close_price=240.0,
        reference_close=245.0,
        change_15m_pct=change_15m,
        volume_15m_ratio=ratio_15m,
    )


class RapidMoveMessageTest(unittest.TestCase):
    def test_rapid_move_message_passes_audit(self):
        from investing_monitor.domain.models import RapidMoveSignal
        from investing_monitor.domain.policies import assess_relative_performance
        from investing_monitor.presentation.quality import audit_message
        from investing_monitor.presentation.slack_messages import (
            build_rapid_move_message,
        )

        sample = frame(-1.8, 3.4)
        signal = RapidMoveSignal(
            event_key="VRT:2026-10-09:rapid:down:1800",
            ticker="VRT",
            trading_date=TODAY,
            direction=Direction.DOWN,
            change_15m_pct=-1.8,
            volume_15m_ratio=3.4,
            observed_at=sample.snapshot.observed_at,
        )

        payload = build_rapid_move_message(
            signal,
            sample.snapshot,
            assess_relative_performance(sample.snapshot),
            (),
            detected_at=AT + timedelta(minutes=2),
            price=240.0,
            reference_close=245.0,
            position=Position(average_price=355.0),
        )

        self.assertTrue(audit_message("rapid_move", payload).passed)
        self.assertIn("15분 급락 감지 -1.8%", payload["text"])
        self.assertIn("3.4배", payload["text"])


class RapidMovePolicyTest(unittest.TestCase):
    def test_fires_on_fast_move_with_volume_burst_and_cooldown(self):
        policy = RapidMovePolicy()
        state = PriceBandState(trading_date=TODAY)

        first, state = policy.evaluate(frame(-1.8, 3.2), state)
        muted, state = policy.evaluate(frame(-2.1, 4.0, minute=10), state)
        opposite, state = policy.evaluate(frame(1.7, 3.0, minute=20), state)
        after_cooldown, state = policy.evaluate(frame(-1.6, 2.6, minute=70), state)

        self.assertIsNotNone(first)
        self.assertEqual(first.direction, Direction.DOWN)
        self.assertIsNone(muted)
        self.assertIsNotNone(opposite)
        self.assertEqual(opposite.direction, Direction.UP)
        self.assertIsNotNone(after_cooldown)

    def test_requires_both_speed_and_volume(self):
        policy = RapidMovePolicy()
        state = PriceBandState(trading_date=TODAY)

        slow, state = policy.evaluate(frame(-0.9, 5.0), state)
        quiet, state = policy.evaluate(frame(-2.5, 1.2), state)
        missing, _ = policy.evaluate(frame(None, None), state)

        self.assertIsNone(slow)
        self.assertIsNone(quiet)
        self.assertIsNone(missing)


    def test_levels_payload_is_json_serializable(self):
        levels = compute_price_levels(
            "VRT",
            history([100.0 + index for index in range(60)]),
            trading_date=TODAY,
            computed_at=AT,
        )

        payload = json.dumps(levels.as_dict(), ensure_ascii=False)

        self.assertEqual(PriceLevels.from_dict(json.loads(payload)), levels)


if __name__ == "__main__":
    unittest.main()
