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

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.monitor import MarketMonitorService, OutboxDeliveryService
from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSession,
    MarketSnapshot,
    PriceBandState,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    PriceBandPolicy,
    RelativeOutcome,
    assess_intraday_volume,
    assess_relative_performance,
)
from investing_monitor.ports.providers import DeliveryOutcomeUnknown
from investing_monitor.presentation.slack_messages import build_price_band_message


OBSERVED_AT = datetime(2026, 9, 2, 14, 31, tzinfo=timezone.utc)
TRADING_DATE = date(2026, 9, 2)


def snapshot(change_pct: float, **overrides) -> MarketSnapshot:
    values = {
        "ticker": "VRT",
        "trading_date": TRADING_DATE,
        "observed_at": OBSERVED_AT,
        "session": MarketSession.REGULAR,
        "change_pct": change_pct,
        "benchmark_change_pct": 1.2,
        "peer_changes": {"ETN": 0.8, "GEV": 1.0, "NVT": 1.3},
    }
    values.update(overrides)
    return MarketSnapshot(**values)


class PriceBandPolicyTest(unittest.TestCase):
    def test_escalates_only_after_entering_a_new_integer_band(self):
        policy = PriceBandPolicy(start_level=4, step=1)
        state = None

        first, state = policy.evaluate(snapshot(4.4), state)
        same_band, state = policy.evaluate(snapshot(4.7), state)
        next_band, state = policy.evaluate(snapshot(5.0), state)
        retracement, state = policy.evaluate(snapshot(4.9), state)

        self.assertEqual(first.level, 4)
        self.assertIsNone(same_band)
        self.assertEqual(next_band.level, 5)
        self.assertIsNone(retracement)
        self.assertEqual(state.upward_high_watermark, 5)

    def test_large_first_observation_sends_one_current_band_alert(self):
        signal, state = PriceBandPolicy().evaluate(snapshot(7.8), None)

        self.assertEqual(signal.level, 7)
        self.assertEqual(state.upward_high_watermark, 7)

    def test_opposite_direction_is_a_distinct_reversal_event(self):
        policy = PriceBandPolicy()
        _, state = policy.evaluate(snapshot(5.1), None)
        signal, state = policy.evaluate(snapshot(-4.2), state)

        self.assertEqual(signal.direction, Direction.DOWN)
        self.assertTrue(signal.is_reversal)
        self.assertEqual(state.upward_high_watermark, 5)
        self.assertEqual(state.downward_high_watermark, 4)

    def test_new_trading_date_resets_both_high_watermarks(self):
        old_state = PriceBandState(
            trading_date=TRADING_DATE,
            upward_high_watermark=8,
            downward_high_watermark=4,
        )
        next_day = snapshot(
            4.1,
            trading_date=TRADING_DATE + timedelta(days=1),
        )
        signal, state = PriceBandPolicy().evaluate(next_day, old_state)

        self.assertEqual(signal.level, 4)
        self.assertEqual(state.upward_high_watermark, 4)
        self.assertEqual(state.downward_high_watermark, 0)


class ContextPolicyTest(unittest.TestCase):
    def test_relative_performance_uses_equal_weight_available_peers(self):
        assessment = assess_relative_performance(snapshot(4.4))

        self.assertEqual(assessment.benchmark, RelativeOutcome.OUTPERFORM)
        self.assertEqual(assessment.peers, RelativeOutcome.OUTPERFORM)
        self.assertEqual(assessment.peer_symbols, ("ETN", "GEV", "NVT"))
        self.assertAlmostEqual(assessment.peer_average_change_pct, 3.1 / 3)

    def test_peer_result_is_hidden_when_fewer_than_two_peers_are_available(self):
        assessment = assess_relative_performance(
            snapshot(4.4, peer_changes={"ETN": 0.8, "GEV": None, "NVT": None})
        )

        self.assertEqual(assessment.peers, RelativeOutcome.UNAVAILABLE)
        self.assertIsNone(assessment.peer_average_change_pct)

    def test_intraday_volume_requires_same_time_baseline(self):
        insufficient = VolumeSnapshot(1_700_000, 1_000_000, baseline_sessions=9)
        ready = VolumeSnapshot(1_700_000, 1_000_000, baseline_sessions=20)

        self.assertFalse(assess_intraday_volume(insufficient).is_ready)
        self.assertTrue(assess_intraday_volume(ready).is_exploded)


class MessageContractTest(unittest.TestCase):
    def test_price_message_preserves_context_without_exact_return_or_price(self):
        policy = PriceBandPolicy()
        market = snapshot(4.73)
        signal, _ = policy.evaluate(market, None)
        relative = assess_relative_performance(market)
        volume = VolumeSnapshot(3_095_486, 1_856_146, baseline_sessions=20)
        catalyst = Catalyst(
            canonical_id="vrt-order-20260902",
            headline="대형 데이터센터 전력·냉각 수주 발표",
            summary="회사는 신규 수주 규모와 납품 시점을 공개했다.",
            source_name="Vertiv IR",
            source_url="https://investors.vertiv.com/example",
            published_at=OBSERVED_AT - timedelta(minutes=10),
            impact=ThesisImpact.STRENGTHEN,
            confidence="high",
        )

        payload = build_price_band_message(
            signal,
            relative,
            volume,
            assess_intraday_volume(volume),
            [catalyst],
        )
        rendered = json.dumps(payload, ensure_ascii=False)

        self.assertIn("+4.0% 상승 구간 진입", rendered)
        self.assertIn("반도체 지수", rendered)
        self.assertIn("피어(ETN·GEV·NVT)", rendered)
        self.assertIn("1.7배", rendered)
        self.assertIn("https://investors.vertiv.com/example", rendered)
        self.assertNotIn("4.73", rendered)
        self.assertNotIn("데이터 상태", rendered)
        self.assertNotIn("내용 확인 필요", rendered)


class RepositoryContractTest(unittest.TestCase):
    def test_state_and_outbox_survive_restart_and_duplicate_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            service = MarketMonitorService(repository)

            event_key = service.handle_snapshot(snapshot(4.4))
            duplicate = service.handle_snapshot(snapshot(4.8))

            restarted = SQLiteMonitorRepository(path)
            persisted = restarted.load_price_band_state("VRT")
            pending = restarted.pending_deliveries(OBSERVED_AT + timedelta(days=1))

            self.assertIsNotNone(event_key)
            self.assertIsNone(duplicate)
            self.assertEqual(persisted.upward_high_watermark, 4)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].event_key, event_key)
            with closing(sqlite3.connect(path)) as connection, connection:
                created_at, next_attempt_at = connection.execute(
                    "SELECT alerts.created_at, outbox.next_attempt_at "
                    "FROM alerts JOIN outbox USING (event_key)"
                ).fetchone()
            self.assertEqual(created_at, OBSERVED_AT.isoformat())
            self.assertEqual(next_attempt_at, OBSERVED_AT.isoformat())

    def test_delivery_failure_is_rescheduled_without_losing_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = MarketMonitorService(repository)
            service.handle_snapshot(snapshot(-4.2))
            pending = repository.pending_deliveries(OBSERVED_AT + timedelta(days=1))
            retry_at = OBSERVED_AT + timedelta(days=1, minutes=1)

            repository.mark_failed(pending[0].outbox_id, retry_at, "temporary Slack error")

            before_retry = repository.pending_deliveries(retry_at - timedelta(seconds=1))
            at_retry = repository.pending_deliveries(retry_at)
            self.assertEqual(before_retry, [])
            self.assertEqual(at_retry[0].attempts, 1)
            self.assertIn(
                "-4.0% 하락 구간 진입",
                json.dumps(at_retry[0].payload, ensure_ascii=False),
            )


class DeliveryContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_ambiguous_provider_failure_is_not_retried_automatically(self):
        class AmbiguousNotifier:
            async def send(self, _payload):
                raise DeliveryOutcomeUnknown("timeout after request body was sent")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            MarketMonitorService(repository).handle_snapshot(snapshot(4.4))
            service = OutboxDeliveryService(repository, AmbiguousNotifier())

            delivered = await service.deliver_pending()

            self.assertEqual(delivered, 0)
            self.assertEqual(
                repository.pending_deliveries(OBSERVED_AT + timedelta(days=1)),
                [],
            )
            with closing(sqlite3.connect(path)) as connection, connection:
                status, attempts = connection.execute(
                    "SELECT delivery_status, attempts FROM outbox"
                ).fetchone()
            self.assertEqual(status, "delivery_unknown")
            self.assertEqual(attempts, 1)

    async def test_remote_checkpoint_brackets_the_slack_call(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            MarketMonitorService(repository).handle_snapshot(snapshot(4.4))
            transitions = []

            def checkpoint_state(reason):
                with closing(sqlite3.connect(path)) as connection, connection:
                    status = connection.execute(
                        "SELECT delivery_status FROM outbox"
                    ).fetchone()[0]
                transitions.append((reason.split(":", 1)[0], status))

            class Notifier:
                async def send(self, _payload):
                    transitions.append(("slack", "called"))
                    return "ok"

            delivered = await OutboxDeliveryService(
                repository,
                Notifier(),
                checkpoint=checkpoint_state,
            ).deliver_pending()

            self.assertEqual(delivered, 1)
            self.assertEqual(
                transitions,
                [
                    ("sending", "sending"),
                    ("slack", "called"),
                    ("delivered", "delivered"),
                ],
            )

    async def test_failed_pre_send_checkpoint_prevents_slack_call(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            MarketMonitorService(repository).handle_snapshot(snapshot(4.4))

            class Notifier:
                called = False

                async def send(self, _payload):
                    self.called = True
                    return "ok"

            notifier = Notifier()

            def fail_checkpoint(_reason):
                raise RuntimeError("remote state unavailable")

            service = OutboxDeliveryService(
                repository,
                notifier,
                checkpoint=fail_checkpoint,
            )
            with self.assertRaisesRegex(RuntimeError, "remote state unavailable"):
                await service.deliver_pending()

            self.assertFalse(notifier.called)
            with closing(sqlite3.connect(path)) as connection, connection:
                status, error = connection.execute(
                    "SELECT delivery_status, last_error FROM outbox"
                ).fetchone()
            self.assertEqual(status, "failed")
            self.assertIn("pre-send checkpoint failed", error)


if __name__ == "__main__":
    unittest.main()
