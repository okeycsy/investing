from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.briefs import CloseBriefService
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import (
    Catalyst,
    MarketCycle,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    assess_intraday_volume,
    assess_relative_performance,
)
from investing_monitor.presentation.close_messages import build_close_message


TRADING_DATE = date(2026, 9, 2)
OBSERVED_AT = datetime(2026, 9, 2, 19, 55, tzinfo=timezone.utc)
OPEN_AT = datetime(2026, 9, 2, 13, 30, tzinfo=timezone.utc)
CREATED_AT = datetime(2026, 9, 2, 20, 15, tzinfo=timezone.utc)


def market_cycle() -> MarketCycle:
    snapshot = MarketSnapshot(
        ticker="VRT",
        trading_date=TRADING_DATE,
        observed_at=OBSERVED_AT,
        session=MarketSession.REGULAR,
        change_pct=2.5,
        benchmark_change_pct=1.0,
        benchmark_symbol="SOXX",
        peer_changes={"ETN": 1.5, "GEV": 1.2, "NVT": 1.1},
    )
    return MarketCycle(
        ticker="VRT",
        trading_date=TRADING_DATE,
        frames=(MarketFrame(snapshot, 102.5, 100.0, 1_200_000),),
        volume=VolumeSnapshot(
            observed_volume=1_200_000,
            expected_volume=1_000_000,
            baseline_sessions=20,
        ),
        source_age_seconds=0,
    )


class CloseBriefServiceTest(unittest.TestCase):
    def test_close_brief_is_ordered_idempotent_and_shadow_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            MarketCycleService(repository, enqueue_alerts=False).process(market_cycle())
            service = CloseBriefService(repository, enqueue_alerts=False)

            first = service.process(
                "VRT",
                TRADING_DATE,
                trading_open_at=OPEN_AT,
                created_at=CREATED_AT,
            )
            second = service.process(
                "VRT",
                TRADING_DATE,
                trading_open_at=OPEN_AT,
                created_at=CREATED_AT,
            )

            rendered = json.dumps(first.payload, ensure_ascii=False)
            direction_at = rendered.index("종목 방향 · 양전")
            benchmark_at = rendered.index("반도체 지수(SOXX) 대비 아웃퍼폼")
            peers_at = rendered.index("피어 평균(ETN·GEV·NVT) 대비 아웃퍼폼")
            volume_at = rendered.index("거래량 평시 범위")
            self.assertLess(direction_at, benchmark_at)
            self.assertLess(benchmark_at, peers_at)
            self.assertLess(peers_at, volume_at)
            self.assertIn("당일 1,200,000주", rendered)
            self.assertIn("최근 20거래일 평균 1,000,000주", rendered)
            self.assertNotIn("102.5", rendered)
            for forbidden in ("DCA", "RSI", "MACD", "PCR", "FINRA", "점수"):
                self.assertNotIn(forbidden, rendered)
            self.assertTrue(first.inserted)
            self.assertFalse(second.inserted)
            self.assertEqual(first.event_key, "VRT:2026-09-02:close")
            self.assertEqual(repository.pending_deliveries(CREATED_AT), [])

            with closing(sqlite3.connect(repository.path)) as connection, connection:
                stored_volume = connection.execute(
                    "SELECT observed_volume, expected_volume, baseline_sessions "
                    "FROM market_volume_observations"
                ).fetchone()
                close_count = connection.execute(
                    "SELECT count(*) FROM alerts WHERE alert_type = 'daily_close'"
                ).fetchone()[0]
            self.assertEqual(stored_volume, (1_200_000, 1_000_000, 20))
            self.assertEqual(close_count, 1)

    def test_missing_market_context_fails_instead_of_sending_empty_brief(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")

            with self.assertRaisesRegex(RuntimeError, "close market context unavailable"):
                CloseBriefService(repository, enqueue_alerts=False).process(
                    "VRT",
                    TRADING_DATE,
                    trading_open_at=OPEN_AT,
                    created_at=CREATED_AT,
                )


class CloseMessageTest(unittest.TestCase):
    def test_thesis_label_requires_high_confidence(self):
        cycle = market_cycle()
        snapshot = cycle.frames[-1].snapshot
        high = Catalyst(
            canonical_id="high",
            headline="대형 고객 계약 해지",
            summary="회사가 주요 고객의 계약 해지를 공시했다.",
            source_name="SEC",
            source_url="https://www.sec.gov/example",
            published_at=CREATED_AT,
            impact=ThesisImpact.DAMAGE,
            confidence="high",
        )
        medium = Catalyst(
            canonical_id="medium",
            headline="업계 수요 둔화 가능성",
            summary="업계 매체가 수요 둔화 가능성을 보도했다.",
            source_name="Industry News",
            source_url="https://example.com/news",
            published_at=CREATED_AT,
            impact=ThesisImpact.DAMAGE,
            confidence="medium",
        )

        high_payload = build_close_message(
            snapshot,
            assess_relative_performance(snapshot),
            cycle.volume,
            assess_intraday_volume(cycle.volume),
            (high,),
        )
        medium_payload = build_close_message(
            snapshot,
            assess_relative_performance(snapshot),
            cycle.volume,
            assess_intraday_volume(cycle.volume),
            (medium,),
        )

        self.assertIn("논지 훼손 근거", json.dumps(high_payload, ensure_ascii=False))
        rendered_medium = json.dumps(medium_payload, ensure_ascii=False)
        self.assertIn("주요 이벤트", rendered_medium)
        self.assertNotIn("논지 훼손 근거", rendered_medium)


if __name__ == "__main__":
    unittest.main()
