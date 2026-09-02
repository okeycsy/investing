from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.briefs import WeeklyBriefService
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.domain.models import (
    Catalyst,
    CloseMarketContext,
    MarketCycle,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    OfficialEvent,
    ThesisImpact,
    VolumeSnapshot,
)


PERIOD_START = date(2026, 8, 31)
PERIOD_END = date(2026, 9, 4)
EVIDENCE_SINCE = datetime(2026, 8, 31, 4, 0, tzinfo=timezone.utc)
CREATED_AT = datetime(2026, 9, 6, 23, 13, tzinfo=timezone.utc)


def visible_text(payload: dict) -> str:
    return payload["text"] + "".join(
        block.get("text", {}).get("text", "")
        + "".join(element.get("text", "") for element in block.get("elements", []))
        for block in payload["blocks"]
    )


def context_for(value: date, change: float = 1.0) -> CloseMarketContext:
    snapshot = MarketSnapshot(
        ticker="VRT",
        trading_date=value,
        observed_at=datetime(value.year, value.month, value.day, 19, 55, tzinfo=timezone.utc),
        session=MarketSession.REGULAR,
        change_pct=change,
        benchmark_change_pct=0.2,
        benchmark_symbol="SOXX",
        peer_changes={"ETN": 0.4, "GEV": 0.3, "NVT": 0.5},
    )
    return CloseMarketContext(
        snapshot=snapshot,
        volume=VolumeSnapshot(1_200_000, 1_000_000, baseline_sessions=20),
    )


def cycle_for(value: date, change: float = 1.0) -> MarketCycle:
    context = context_for(value, change)
    return MarketCycle(
        ticker="VRT",
        trading_date=value,
        frames=(
            MarketFrame(
                context.snapshot,
                close_price=100.0 + change,
                reference_close=100.0,
                cumulative_volume=context.volume.observed_volume,
            ),
        ),
        volume=context.volume,
        source_age_seconds=0,
    )


class WeeklyBriefIntegrationTest(unittest.TestCase):
    def test_weekly_uses_only_requested_calendar_week_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            market = MarketCycleService(repository, enqueue_alerts=False)
            market.process(cycle_for(date(2026, 8, 28), change=-9.0))
            for day in range(31, 36):
                value = date(2026, 8, day) if day == 31 else date(2026, 9, day - 31)
                market.process(cycle_for(value))
            service = WeeklyBriefService(repository, enqueue_alerts=False)

            first = service.process(
                "VRT",
                PERIOD_START,
                PERIOD_END,
                evidence_since=EVIDENCE_SINCE,
                created_at=CREATED_AT,
            )
            second = service.process(
                "VRT",
                PERIOD_START,
                PERIOD_END,
                evidence_since=EVIDENCE_SINCE,
                created_at=CREATED_AT,
            )

            rendered = json.dumps(first.payload, ensure_ascii=False)
            self.assertEqual(first.market_sessions, 5)
            self.assertIn("완료된 정규장 5거래일 기준", rendered)
            self.assertIn("주간 방향 · 상승", rendered)
            self.assertIn("반도체 지수(SOXX) 대비 아웃퍼폼", rendered)
            self.assertIn("피어 평균(ETN·GEV·NVT) 대비 아웃퍼폼", rendered)
            self.assertNotIn("-9.0", rendered)
            self.assertTrue(first.inserted)
            self.assertFalse(second.inserted)
            self.assertEqual(first.event_key, "VRT:2026-W36:weekly")
            self.assertEqual(repository.pending_deliveries(CREATED_AT), [])

    def test_weekly_refuses_incomplete_market_history(self):
        class SparseRepository:
            def load_close_market_contexts(self, *_args):
                return (context_for(PERIOD_END),)

        with self.assertRaisesRegex(RuntimeError, "only 1 completed sessions"):
            WeeklyBriefService(SparseRepository(), enqueue_alerts=False).process(
                "VRT",
                PERIOD_START,
                PERIOD_END,
                evidence_since=EVIDENCE_SINCE,
                created_at=CREATED_AT,
            )


class WeeklyEvidenceTest(unittest.TestCase):
    def test_weekly_deduplicates_events_and_only_labels_high_confidence(self):
        strengthening = Catalyst(
            canonical_id="contract",
            headline="대형 데이터센터 계약 수주",
            summary=(
                "회사가 계약 규모와 납기 계획을 공개했다. " + "상세 근거 " * 300
            ),
            source_name="Vertiv IR",
            source_url="https://example.com/contract",
            published_at=CREATED_AT,
            impact=ThesisImpact.STRENGTHEN,
            confidence="high",
            facts=("계약 규모는 10억 달러다. " + "공식 수치 " * 200,),
            source_kind="ir",
        )
        duplicate = Catalyst(
            canonical_id="contract",
            headline="같은 계약의 재전재",
            summary="같은 사건이다.",
            source_name="News",
            source_url="https://example.com/duplicate",
            published_at=CREATED_AT,
            impact=ThesisImpact.STRENGTHEN,
            confidence="high",
        )
        risk = Catalyst(
            canonical_id="risk",
            headline="핵심 고객 계약 해지",
            summary="회사가 핵심 고객 이탈을 공시했다. " + "위험 설명 " * 300,
            source_name="SEC",
            source_url="https://www.sec.gov/example",
            published_at=CREATED_AT,
            impact=ThesisImpact.DAMAGE,
            confidence="high",
            source_kind="sec",
        )
        weak = Catalyst(
            canonical_id="weak",
            headline="수요 둔화 가능성",
            summary="업계 매체의 추정이다.",
            source_name="News",
            source_url="https://example.com/weak",
            published_at=CREATED_AT,
            impact=ThesisImpact.RISK,
            confidence="medium",
        )

        class Repository:
            def __init__(self):
                self.alerts = []

            def load_close_market_contexts(self, *_args):
                return tuple(
                    context_for(date(2026, 9, day)) for day in (1, 2, 3, 4)
                )

            def recent_catalysts(self, *_args, **_kwargs):
                return [strengthening, duplicate, risk, weak]

            def upcoming_official_events(self, *_args, **_kwargs):
                return [
                    OfficialEvent(
                        event_date=date(2026, 9, 8),
                        title_ko="투자자 컨퍼런스",
                        source_url="https://investors.vertiv.com/event",
                        source_text="September 8, 2026",
                        time_et="09:00",
                    )
                ]

            def record_alert(self, alert, *, enqueue=True):
                self.alerts.append((alert, enqueue))
                return True

        report = WeeklyBriefService(Repository(), enqueue_alerts=False).process(
            "VRT",
            PERIOD_START,
            PERIOD_END,
            evidence_since=EVIDENCE_SINCE,
            created_at=CREATED_AT,
        )

        rendered = json.dumps(report.payload, ensure_ascii=False)
        self.assertEqual(report.strengthening_count, 1)
        self.assertEqual(report.risk_count, 1)
        self.assertEqual(rendered.count("대형 데이터센터 계약 수주"), 1)
        self.assertNotIn("같은 계약의 재전재", rendered)
        self.assertNotIn("수요 둔화 가능성", rendered)
        self.assertIn("계약 규모는 10억 달러다.", rendered)
        self.assertIn("다음 주 공식 일정", rendered)
        self.assertIn("09/08 투자자 컨퍼런스", rendered)
        self.assertIn("09:00 ET", rendered)
        self.assertIn("https://investors.vertiv.com/event", rendered)
        self.assertEqual(report.upcoming_event_count, 1)
        self.assertLessEqual(len(visible_text(report.payload)), 2_900)
        for forbidden in ("DCA", "RSI", "MACD", "PCR", "FINRA", "점수"):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()
