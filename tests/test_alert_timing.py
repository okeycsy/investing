from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION, SQLiteMonitorRepository
from investing_monitor.application.briefs import CloseBriefService
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import (
    MarketCycle, MarketFrame, MarketSession, MarketSnapshot, PriceBandState, VolumeSnapshot,
)
from investing_monitor.presentation.quality import audit_message


DAY = date(2026, 9, 17)
EVENT_AT = datetime(2026, 9, 17, 12, 30, tzinfo=timezone.utc)
LATEST_AT = datetime(2026, 9, 17, 14, 55, tzinfo=timezone.utc)
DETECTED_AT = datetime(2026, 9, 17, 14, 57, tzinfo=timezone.utc)


def frame(at: datetime, change: float, session=MarketSession.REGULAR) -> MarketFrame:
    return MarketFrame(
        MarketSnapshot(
            ticker="VRT", trading_date=DAY, observed_at=at, session=session,
            change_pct=change, benchmark_change_pct=1.0,
            peer_changes={"ETN": 1.0, "GEV": 1.0, "NVT": 1.0},
        ),
        close_price=100 + change, reference_close=100,
    )


def cycle(*frames: MarketFrame, volume: VolumeSnapshot | None = None) -> MarketCycle:
    return MarketCycle("VRT", DAY, tuple(frames), volume, 0)


def rendered(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


class AlertTimingTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repository = SQLiteMonitorRepository(Path(self.directory.name) / "monitor.db")
        self.service = MarketCycleService(self.repository, enqueue_alerts=False)

    def test_premarket_event_regular_latest_and_volume_have_separate_times(self):
        report = self.service.process(cycle(
            frame(EVENT_AT, 4.25, MarketSession.PRE), frame(LATEST_AT, -1.25),
            volume=VolumeSnapshot(1_798_281, 1_584_886, 20, observed_at=LATEST_AT),
        ), detected_at=DETECTED_AT)
        payload = report.messages[0]
        text = rendered(payload)
        self.assertIn("+4.0% 상승 도달 기록 · 지연 확인", text)
        self.assertIn("프리마켓 · 관측 09/17 21:30 KST", text)
        self.assertIn("봇 확인 09/17 23:57 KST", text)
        self.assertIn("마지막 관측 · 정규장 09/17 23:55 KST", text)
        self.assertIn("음전 · +4.0% 구간 아래로 되돌림", text)
        self.assertIn("반도체 지수(SOXX) 대비 강한 언더퍼폼", text)
        self.assertNotIn("아웃퍼폼", text)
        self.assertIn("정규장 누적 · 09/17 23:55 KST 봉까지", text)
        self.assertNotIn("거래량 동반", text)
        self.assertNotIn("-1.25%", text)
        self.assertNotIn("98.75", text)
        self.assertTrue(audit_message("price_band", payload).passed)
        for value in ("지연 확인", "21:30 KST", "23:57 KST", "23:55 KST", "되돌림"):
            self.assertIn(value, payload["text"])
        self.assertEqual(self.repository.pending_deliveries(DETECTED_AT), [])
        again = self.service.process(cycle(
            frame(EVENT_AT, 4.25, MarketSession.PRE), frame(LATEST_AT, -1.25),
        ), detected_at=DETECTED_AT)
        self.assertEqual(again.inserted_event_keys, ())
        with closing(sqlite3.connect(self.repository.path)) as connection:
            context = json.loads(connection.execute("SELECT context_json FROM alerts").fetchone()[0])
        self.assertEqual(context["timing"]["volume_observed_at"], LATEST_AT.isoformat())
        self.assertEqual(context["timing"]["detected_at"], DETECTED_AT.isoformat())

    def test_delay_boundary_and_live_title(self):
        for seconds, delayed in ((599, False), (600, False), (601, True)):
            with self.subTest(seconds=seconds), tempfile.TemporaryDirectory() as directory:
                service = MarketCycleService(SQLiteMonitorRepository(Path(directory) / "test.db"))
                payload = service.process(
                    cycle(frame(EVENT_AT, 4.25)),
                    detected_at=EVENT_AT + timedelta(seconds=seconds),
                ).messages[0]
                self.assertEqual("지연 확인" in payload["text"], delayed)
                self.assertEqual("도달 기록" in payload["text"], delayed)
                self.assertEqual("구간 진입" in payload["text"], not delayed)
                self.assertIn("관측 09/17 21:30 KST", payload["text"])
                self.assertIn("확인", payload["text"])

    def test_recovered_up_and_down_status_is_symmetric_and_inclusive(self):
        for event, latest, expected in (
            (4.25, 4.0, "+4.0% 구간 유지"),
            (4.25, 1.25, "+4.0% 구간 아래로 되돌림"),
            (-4.25, -4.0, "-4.0% 구간 유지"),
            (-4.25, -1.25, "-4.0% 구간에서 반등"),
            (-4.25, 1.25, "양전 · -4.0% 구간에서 반등"),
            (4.25, -1.25, "음전 · +4.0% 구간 아래로 되돌림"),
        ):
            with self.subTest(event=event, latest=latest), tempfile.TemporaryDirectory() as directory:
                service = MarketCycleService(SQLiteMonitorRepository(Path(directory) / "test.db"))
                report = service.process(cycle(
                    frame(EVENT_AT, event), frame(LATEST_AT, latest),
                ), detected_at=DETECTED_AT)
                self.assertEqual(len(report.messages), 1)
                self.assertIn(expected, rendered(report.messages[0]))

    def test_last_observation_is_not_described_as_current_when_stale(self):
        payload = self.service.process(cycle(
            frame(EVENT_AT, 4.25), frame(EVENT_AT + timedelta(minutes=5), 1.25),
        ), detected_at=DETECTED_AT).messages[0]
        self.assertIn("마지막 자료도 봇 확인보다 2시간 22분 이전", rendered(payload))
        self.assertIn("이후 상태는 확인되지 않음", rendered(payload))
        self.assertNotIn("현재", rendered(payload))

    def test_late_volume_uses_regular_data_time_not_postmarket_time(self):
        volume_at = datetime(2026, 9, 17, 19, 55, tzinfo=timezone.utc)
        post_at = datetime(2026, 9, 17, 22, 0, tzinfo=timezone.utc)
        report = self.service.process(cycle(
            frame(post_at, 1.25, MarketSession.POST),
            volume=VolumeSnapshot(2_000, 1_000, 20, observed_at=volume_at),
        ), detected_at=post_at + timedelta(minutes=1))
        payload = report.messages[0]
        self.assertIn("자료 09/18 04:55 KST", payload["text"])
        self.assertIn("확인 09/18 07:01 KST", payload["text"])
        self.assertIn("시장 관측 · 애프터마켓 09/18 07:00 KST", rendered(payload))
        self.assertEqual(report.max_detection_delay_seconds, 126 * 60)
        self.assertTrue(audit_message("volume_spike", payload).passed)

    def test_close_persists_regular_volume_asof_through_restart(self):
        regular_at = datetime(2026, 9, 17, 19, 55, tzinfo=timezone.utc)
        post_at = datetime(2026, 9, 17, 22, 0, tzinfo=timezone.utc)
        self.service.process(cycle(
            frame(regular_at, 1.25), frame(post_at, 4.25, MarketSession.POST),
            volume=VolumeSnapshot(2_000, 1_000, 20, observed_at=regular_at),
        ))
        restarted = SQLiteMonitorRepository(self.repository.path)
        self.assertEqual(restarted.load_close_market_context("VRT", DAY).volume.observed_at, regular_at)
        report = CloseBriefService(restarted, enqueue_alerts=False).process(
            "VRT", DAY, trading_open_at=EVENT_AT, created_at=post_at + timedelta(minutes=1),
        )
        text = rendered(report.payload)
        self.assertIn("미국 거래일 09/17 · 정규장", text)
        self.assertIn("시장 자료 09/18 04:55 KST", text)
        self.assertIn("브리프 작성 09/18 07:01 KST", text)
        self.assertIn("정규장 누적 · 09/18 04:55 KST 봉까지", text)
        self.assertNotIn("애프터마켓", text)
        self.assertTrue(audit_message("daily_close", report.payload).passed)

    def test_timezone_aware_volume_required_and_dst_conversion(self):
        with self.assertRaises(ValueError):
            VolumeSnapshot(2_000, 1_000, 20, observed_at=datetime(2026, 9, 17))
        from investing_monitor.presentation.timing import delay_seconds, timestamp
        self.assertEqual(timestamp(datetime(2026, 12, 1, 16, tzinfo=ZoneInfo("America/New_York"))),
                         "12/02 06:00 KST")
        ny = ZoneInfo("America/New_York")
        self.assertEqual(delay_seconds(
            datetime(2026, 11, 1, 1, 59, tzinfo=ny, fold=0),
            datetime(2026, 11, 1, 1, 1, tzinfo=ny, fold=1),
        ), 120)

    def test_v10_migration_does_not_invent_legacy_volume_data_time(self):
        path = Path(self.directory.name) / "legacy.db"
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.executescript("""
                CREATE TABLE market_volume_observations (
                    ticker TEXT NOT NULL, observed_at TEXT NOT NULL, trading_date TEXT NOT NULL,
                    observed_volume INTEGER NOT NULL, expected_volume INTEGER NOT NULL,
                    baseline_sessions INTEGER NOT NULL, lookback_sessions INTEGER NOT NULL,
                    PRIMARY KEY(ticker, observed_at)
                );
                PRAGMA user_version = 10;
            """)
            connection.execute("INSERT INTO market_volume_observations VALUES (?, ?, ?, ?, ?, ?, ?)",
                               ("VRT", LATEST_AT.isoformat(), DAY.isoformat(), 2_000, 1_000, 20, 20))
        migrated = SQLiteMonitorRepository(path)
        migrated.record_market_cycle("VRT", PriceBandState(DAY), (frame(LATEST_AT, 1.25),), None, ())
        context = migrated.load_close_market_context("VRT", DAY)
        self.assertIsNone(context.volume.observed_at)
        self.assertEqual(context.volume.observed_volume, 2_000)
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)


if __name__ == "__main__":
    unittest.main()
