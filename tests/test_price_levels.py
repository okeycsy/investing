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
from investing_monitor.presentation.levels import levels_text, position_text

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
