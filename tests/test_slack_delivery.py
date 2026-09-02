from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.slack import SlackWebhookNotifier
from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.monitor import OutboxDeliveryService
from investing_monitor.ports.providers import DeliveryOutcomeUnknown, DeliveryRejected
from investing_monitor.ports.repository import AlertRecord


NOW = datetime(2026, 9, 2, 1, 15, tzinfo=timezone.utc)
WEBHOOK = "https://hooks.slack.com/services/test/secret/value"


class Response:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


def valid_close_payload() -> dict:
    return {
        "text": "$VRT 09/02 장 마감 브리프",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "📊 $VRT 장 마감 — 09/02"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "📈 *종목 방향 · 양전*"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "↗️ *반도체 지수(SOXX) 대비 아웃퍼폼*",
                },
            },
        ],
    }


class SlackWebhookNotifierTest(unittest.IsolatedAsyncioTestCase):
    async def test_success_returns_request_receipt(self):
        notifier = SlackWebhookNotifier(
            WEBHOOK,
            post=lambda *_args, **_kwargs: Response(
                200,
                {"x-slack-req-id": "request-123"},
            ),
        )

        receipt = await notifier.send(valid_close_payload())

        self.assertEqual(receipt, "request-123")

    async def test_rate_limit_is_retryable_and_honors_retry_after(self):
        notifier = SlackWebhookNotifier(
            WEBHOOK,
            post=lambda *_args, **_kwargs: Response(429, {"Retry-After": "45"}),
        )

        with self.assertRaises(DeliveryRejected) as raised:
            await notifier.send(valid_close_payload())

        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.retry_after_seconds, 45)

    async def test_client_rejection_is_permanent(self):
        notifier = SlackWebhookNotifier(
            WEBHOOK,
            post=lambda *_args, **_kwargs: Response(404),
        )

        with self.assertRaises(DeliveryRejected) as raised:
            await notifier.send(valid_close_payload())

        self.assertFalse(raised.exception.retryable)

    async def test_timeout_is_ambiguous_without_leaking_webhook(self):
        def timeout(*_args, **_kwargs):
            raise requests.Timeout("request timed out")

        notifier = SlackWebhookNotifier(WEBHOOK, post=timeout)

        with self.assertRaises(DeliveryOutcomeUnknown) as raised:
            await notifier.send(valid_close_payload())

        self.assertNotIn(WEBHOOK, str(raised.exception))


class DeliveryRejectionStateTest(unittest.IsolatedAsyncioTestCase):
    async def test_permanent_rejection_is_preserved_without_retry(self):
        class RejectedNotifier:
            async def send(self, _payload):
                raise DeliveryRejected("invalid webhook", retryable=False)

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            repository.record_alert(
                AlertRecord(
                    event_key="VRT:2026-09-02:close",
                    ticker="VRT",
                    alert_type="daily_close",
                    created_at=NOW,
                    payload=valid_close_payload(),
                )
            )
            checkpoints = []

            delivered = await OutboxDeliveryService(
                repository,
                RejectedNotifier(),
                checkpoint=checkpoints.append,
            ).deliver_pending()

            self.assertEqual(delivered, 0)
            self.assertEqual(
                [item.split(":", 1)[0] for item in checkpoints],
                ["sending", "delivery-discarded"],
            )
            self.assertEqual(repository.pending_deliveries(NOW), [])
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                status = connection.execute(
                    "SELECT delivery_status FROM outbox"
                ).fetchone()[0]
            self.assertEqual(status, "discarded")


if __name__ == "__main__":
    unittest.main()
