from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.application.replay import MarketReplayLab
from investing_monitor.domain.models import (
    MarketCycle,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    VolumeSnapshot,
)


TRADING_DATE = date(2026, 9, 2)


class Calendar:
    def regular_open(self, value):
        return datetime.combine(value, time(13, 30), timezone.utc)

    def regular_close(self, value):
        return datetime.combine(value, time(20, 0), timezone.utc)


class Provider:
    def __init__(self, changes):
        self.changes = changes
        self.calls = []

    def fetch_cycle(self, now, *, last_observed_at):
        self.calls.append((now, last_observed_at))
        frames = tuple(
            MarketFrame(
                MarketSnapshot(
                    ticker="VRT",
                    trading_date=TRADING_DATE,
                    observed_at=datetime(
                        2026,
                        9,
                        2,
                        14,
                        index * 5,
                        tzinfo=timezone.utc,
                    ),
                    session=MarketSession.REGULAR,
                    change_pct=change,
                    benchmark_change_pct=1.0,
                    benchmark_symbol="SOXX",
                    peer_changes={"ETN": 1.0, "GEV": 1.1, "NVT": 0.9},
                ),
                close_price=100.0 + change,
                reference_close=100.0,
                cumulative_volume=100_000 * (index + 1),
            )
            for index, change in enumerate(self.changes)
        )
        return MarketCycle(
            ticker="VRT",
            trading_date=TRADING_DATE,
            frames=frames,
            volume=VolumeSnapshot(500_000, 500_000, baseline_sessions=19),
            source_age_seconds=0,
        )


class MarketReplayLabTest(unittest.TestCase):
    def test_gap_replay_keeps_only_highest_same_direction_band(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "replay.db")
            provider = Provider((4.2, 5.3, 4.7))
            report = MarketReplayLab(
                provider,
                Calendar(),
                MarketCycleService(repository, enqueue_alerts=False),
            ).replay_day(TRADING_DATE)

            self.assertEqual(report.observed_frames, 3)
            self.assertEqual(report.replayed_frames, 2)
            self.assertEqual(
                report.event_keys,
                ("VRT:2026-09-02:price-band:up:5",),
            )
            self.assertEqual(report.repeated_event_keys, ())
            self.assertTrue(report.quality_passed)
            self.assertEqual(report.quality_violations, ())
            self.assertIn(
                "동시간대 19거래일 평균",
                str(report.messages[0]),
            )
            self.assertEqual(len(provider.calls), 1)
            self.assertEqual(
                provider.calls[0][1],
                datetime(2026, 9, 2, 13, 25, tzinfo=timezone.utc),
            )

    def test_true_intraday_reversal_keeps_each_direction_extreme_once(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "replay.db")
            report = MarketReplayLab(
                Provider((4.2, 6.4, -4.1, -5.8, -4.2)),
                Calendar(),
                MarketCycleService(repository, enqueue_alerts=False),
            ).replay_day(TRADING_DATE)

            self.assertEqual(
                report.event_keys,
                (
                    "VRT:2026-09-02:price-band:up:6",
                    "VRT:2026-09-02:price-band:down:5",
                ),
            )
            self.assertEqual(report.repeated_event_keys, ())
            self.assertTrue(report.quality_passed)


if __name__ == "__main__":
    unittest.main()
