from __future__ import annotations

import copy
import io
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.cli import _build_slack_canary, build_parser, main
from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.presentation.previews import PREVIEW_KINDS, build_preview_message
from investing_monitor.presentation.quality import audit_message
from investing_monitor.ports.repository import AlertRecord
from investing_monitor.runtime.tick import PlannedTask, TickPlan, TickTask


NOW = datetime(2026, 9, 4, 14, 31, tzinfo=timezone.utc)


def product_payload() -> dict:
    return {
        "text": "$VRT +4.0% 상승 구간 진입",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📈 $VRT +4.0% 상승 구간 진입",
                },
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


class ProductionCliTest(unittest.TestCase):
    def test_empty_successful_news_polls_are_not_runtime_failures(self):
        class NewsOnlyPlanner:
            def plan(self, now, _checkpoints, *, last_completed_run_at):
                return TickPlan(
                    now=now,
                    gap_seconds=0,
                    tasks=(PlannedTask(TickTask.NEWS, "news"),),
                )

        class EmptySource:
            def fetch(self, _profile):
                return []

        class EmptyReport:
            failed = 0
            analyzed = 0

            def as_dict(self):
                return {
                    "seen": 0,
                    "inserted_pending": 0,
                    "filtered": 0,
                    "quarantined": 0,
                    "enriched": 0,
                    "analyzed": 0,
                    "relevant": 0,
                    "failed": 0,
                    "alerts": 0,
                    "reclassified_low_value": 0,
                    "reconciled_clusters": 0,
                }

        class Ingestion:
            called = False

            def ingest(self, candidates, _now):
                self.called = True
                self.candidates = candidates
                return EmptyReport()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            ingestion = Ingestion()
            output = io.StringIO()
            with (
                patch(
                    "investing_monitor.cli.TickPlanner",
                    side_effect=lambda **_kwargs: NewsOnlyPlanner(),
                ),
                patch("investing_monitor.cli.YahooNewsAdapter", return_value=EmptySource()),
                patch(
                    "investing_monitor.cli.InvestorRelationsFeedAdapter",
                    return_value=EmptySource(),
                ),
                patch(
                    "investing_monitor.cli.EvidenceIngestionService",
                    return_value=ingestion,
                ),
                redirect_stdout(output),
            ):
                result = main(
                    [
                        "--db",
                        str(path),
                        "shadow-tick",
                        "--config",
                        str(ROOT / "monitor_config.md"),
                        "--now",
                        NOW.isoformat(),
                        "--scheduled-at",
                        NOW.isoformat(),
                        "--run-id",
                        "empty-news",
                        "--trigger",
                        "schedule",
                    ]
                )

            self.assertEqual(result, 0, output.getvalue())
            self.assertTrue(ingestion.called)
            self.assertEqual(ingestion.candidates, [])
            self.assertEqual(SQLiteMonitorRepository(path).recent_runs()[0].status, "success")

    def test_canary_is_labeled_without_mutating_product_message(self):
        source = product_payload()
        original = copy.deepcopy(source)

        canary = _build_slack_canary(source)

        self.assertEqual(source, original)
        self.assertTrue(canary["text"].startswith("[V2 검증]"))
        self.assertTrue(
            canary["blocks"][0]["text"]["text"].startswith("V2 검증 ·")
        )
        self.assertIn("투자 신호가 아닙니다", str(canary["blocks"][1]))
        self.assertTrue(audit_message("delivery_canary", canary).passed)

    def test_production_tick_requires_explicit_runtime_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"V2_PRODUCTION_ENABLED": ""}):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "V2_PRODUCTION_ENABLED=true",
                ):
                    main(
                        [
                            "--db",
                            str(Path(directory) / "monitor.db"),
                            "production-tick",
                        ]
                    )

    def test_production_commands_expose_git_state_arguments(self):
        parser = build_parser()

        production = parser.parse_args(
            ["production-tick", "--repository", "/tmp/repository"]
        )
        canary = parser.parse_args(
            ["slack-canary", "--repository", "/tmp/repository"]
        )
        preview = parser.parse_args(
            ["slack-preview", "--repository", "/tmp/repository"]
        )

        self.assertEqual(production.repository, "/tmp/repository")
        self.assertEqual(canary.repository, "/tmp/repository")
        self.assertEqual(preview.repository, "/tmp/repository")

    def test_all_preview_fixtures_are_labeled_and_quality_approved(self):
        for kind in PREVIEW_KINDS:
            with self.subTest(kind=kind):
                payload = build_preview_message(
                    kind,
                    ticker="VRT",
                    benchmark="SOXX",
                    peers=("ETN", "GEV", "NVT"),
                    now=NOW,
                )

                self.assertTrue(payload["text"].startswith("[V2 미리보기]"))
                self.assertIn("실제 투자 신호가 아닙니다", str(payload["blocks"][1]))
                self.assertTrue(audit_message("delivery_canary", payload).passed)

    def test_production_tick_delivers_outbox_with_remote_checkpoints(self):
        class DeliveryOnlyPlanner:
            def plan(self, now, _checkpoints, *, last_completed_run_at):
                return TickPlan(
                    now=now,
                    gap_seconds=0,
                    tasks=(PlannedTask(TickTask.DELIVERY, "delivery"),),
                )

        class AcceptedNotifier:
            async def send(self, _payload):
                return "slack-request-1"

        class StateStore:
            def __init__(self):
                self.run_ids = []

            def checkpoint(self, _database, *, run_id):
                self.run_ids.append(run_id)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            repository.record_alert(
                AlertRecord(
                    event_key="VRT:2026-09-04:price-band:up:4",
                    ticker="VRT",
                    alert_type="price_band",
                    created_at=datetime.now(timezone.utc),
                    payload=product_payload(),
                )
            )
            state_store = StateStore()
            with (
                patch.dict(
                    os.environ,
                    {
                        "V2_PRODUCTION_ENABLED": "true",
                        "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
                    },
                ),
                patch(
                    "investing_monitor.cli.TickPlanner",
                    side_effect=lambda **_kwargs: DeliveryOnlyPlanner(),
                ),
                patch(
                    "investing_monitor.cli.SlackWebhookNotifier",
                    return_value=AcceptedNotifier(),
                ),
                patch("investing_monitor.cli._state_store", return_value=state_store),
            ):
                with redirect_stdout(io.StringIO()):
                    result = main(
                        [
                            "--db",
                            str(path),
                            "production-tick",
                            "--config",
                            str(ROOT / "monitor_config.md"),
                            "--now",
                            NOW.isoformat(),
                            "--scheduled-at",
                            NOW.isoformat(),
                            "--run-id",
                            "production-run",
                        ]
                    )

            with closing(sqlite3.connect(path)) as connection, connection:
                delivery_status, receipt = connection.execute(
                    "SELECT delivery_status, receipt FROM outbox"
                ).fetchone()

            self.assertEqual(result, 0)
            self.assertEqual(delivery_status, "delivered")
            self.assertEqual(receipt, "slack-request-1")
            self.assertEqual(len(state_store.run_ids), 2)
            self.assertIn(":sending:", state_store.run_ids[0])
            self.assertIn(":delivered:", state_store.run_ids[1])


class ProductionWorkflowTest(unittest.TestCase):
    def test_release_workflow_owns_production_schedule_after_cutover(self):
        workflow = (
            ROOT / ".github" / "workflows" / "monitor_v2_production.yml"
        ).read_text(encoding="utf-8")
        shadow = (
            ROOT / ".github" / "workflows" / "monitor_v2_shadow.yml"
        ).read_text(encoding="utf-8")
        legacy = (
            ROOT / ".github" / "workflows" / "hood_monitor.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("\n  schedule:", workflow)
        self.assertIn("4-59/5 4-19 * * 1-5", workflow)
        self.assertIn("24,54 0-3,20-23 * * 1-5", workflow)
        self.assertIn("timezone: 'America/New_York'", workflow)
        self.assertIn("13 8 * * 1", workflow)
        self.assertIn("timezone: 'Asia/Seoul'", workflow)
        self.assertIn("slack-canary", workflow)
        self.assertIn("slack-preview", workflow)
        self.assertIn("preview_kind", workflow)
        self.assertIn("production-tick", workflow)
        self.assertIn("github.event_name == 'schedule'", workflow)
        self.assertIn("V2_PRODUCTION_ENABLED: 'true'", workflow)
        self.assertIn("SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}", workflow)
        self.assertIn("checkpoint-state", workflow)
        self.assertIn("group: ticker-monitor-v2-state", workflow)
        self.assertNotIn("\n  schedule:", shadow)
        self.assertNotIn("\n  schedule:", legacy)

    def test_daily_state_backup_is_isolated_and_retained_for_seven_days(self):
        workflow = (
            ROOT / ".github" / "workflows" / "monitor_v2_backup.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("schedule:", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("group: ticker-monitor-v2-state", workflow)
        self.assertIn("restore-state", workflow)
        self.assertIn("quality-report", workflow)
        self.assertIn("actions/upload-artifact@v4", workflow)
        self.assertIn("path: .runtime/monitor.db", workflow)
        self.assertIn("retention-days: 7", workflow)
        self.assertNotIn("SLACK_WEBHOOK_URL", workflow)


if __name__ == "__main__":
    unittest.main()
