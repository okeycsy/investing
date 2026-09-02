from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.quality import QualityReportService
from investing_monitor.ports.repository import AlertRecord
from investing_monitor.presentation.quality import (
    MessageQualityError,
    audit_message,
)


NOW = datetime(2026, 9, 2, 20, 15, tzinfo=timezone.utc)


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


class MessageQualityGateTest(unittest.TestCase):
    def test_invalid_message_is_rejected_before_alert_or_outbox_insert(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            payload = valid_close_payload()
            payload["blocks"].append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "DCA 계획: 중단 검토"},
                }
            )

            with self.assertRaisesRegex(MessageQualityError, "forbidden user text: DCA"):
                repository.record_alert(
                    AlertRecord(
                        event_key="VRT:2026-09-02:close",
                        ticker="VRT",
                        alert_type="daily_close",
                        created_at=NOW,
                        payload=payload,
                    )
                )

            with closing(sqlite3.connect(repository.path)) as connection, connection:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM alerts").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM outbox").fetchone()[0],
                    0,
                )

    def test_duplicate_visible_block_is_rejected(self):
        payload = valid_close_payload()
        payload["blocks"].append(payload["blocks"][-1])

        result = audit_message("daily_close", payload)

        self.assertFalse(result.passed)
        self.assertIn("duplicate visible block", result.violations)


class QualityReportTest(unittest.TestCase):
    def test_report_combines_message_and_runtime_health(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            repository.record_alert(
                AlertRecord(
                    event_key="VRT:2026-09-02:close",
                    ticker="VRT",
                    alert_type="daily_close",
                    created_at=NOW,
                    payload=valid_close_payload(),
                ),
                enqueue=False,
            )
            repository.start_run(
                "run-1",
                scheduled_at=NOW - timedelta(minutes=12),
                started_at=NOW,
                gap_seconds=15 * 60,
            )
            repository.finish_run(
                "run-1",
                completed_at=NOW + timedelta(seconds=5),
                status="success",
                summary={
                    "plan": {
                        "tasks": [
                            {"name": "market", "checkpoint_key": "market"},
                        ]
                    },
                    "succeeded": ["market"],
                },
            )

            report = QualityReportService(repository).build()

            self.assertTrue(report.passed)
            self.assertEqual(report.messages_checked, 1)
            self.assertEqual(report.runtime["runs_checked"], 1)
            self.assertEqual(report.runtime["max_schedule_delay_seconds"], 12 * 60)
            self.assertEqual(report.runtime["gap_runs_over_10_minutes"], 1)
            self.assertEqual(report.runtime["planned_tasks"], {"market": 1})
            self.assertEqual(report.runtime["succeeded_tasks"], {"market": 1})
            self.assertEqual(
                report.recent_messages[0]["fallback_text"],
                "$VRT 09/02 장 마감 브리프",
            )


if __name__ == "__main__":
    unittest.main()
