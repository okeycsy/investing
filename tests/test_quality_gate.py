from __future__ import annotations

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
from investing_monitor.application.quality import QualityReportService
from investing_monitor.domain.models import (
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    PriceBandState,
)
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


def valid_catalyst_payload(source_url: str, title: str = "중요 회사 사건") -> dict:
    return {
        "text": f"$VRT {title}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{title}*\n확인된 사실과 투자 논지 변화",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"<{source_url}|원문 보기>",
                    }
                ],
            },
        ],
    }


def record_full_market_day(
    repository: SQLiteMonitorRepository,
    trading_date: date,
    *,
    bucket_count: int = 78,
) -> None:
    frames = []
    session_start = datetime.combine(
        trading_date,
        datetime.min.time(),
        tzinfo=timezone.utc,
    ) + timedelta(hours=13, minutes=30)
    for index in range(bucket_count):
        observed_at = session_start + timedelta(minutes=index * 5)
        snapshot = MarketSnapshot(
            ticker="VRT",
            trading_date=trading_date,
            observed_at=observed_at,
            session=MarketSession.REGULAR,
            change_pct=1.0,
            benchmark_symbol="SOXX",
            benchmark_change_pct=0.5,
            peer_changes={"ETN": 0.3, "GEV": 0.7},
        )
        frames.append(
            MarketFrame(
                snapshot=snapshot,
                close_price=101.0,
                reference_close=100.0,
                cumulative_volume=1000 * (index + 1),
            )
        )
    repository.record_market_cycle(
        "VRT",
        PriceBandState(trading_date),
        frames,
        volume=None,
        alerts=(),
    )


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
    def test_market_data_coverage_respects_early_close_calendar(self):
        class EarlyCloseCalendar:
            def regular_open(self, value: date) -> datetime:
                return datetime.combine(
                    value,
                    datetime.min.time(),
                    timezone.utc,
                ) + timedelta(hours=13, minutes=30)

            def regular_close(self, value: date) -> datetime:
                return datetime.combine(
                    value,
                    datetime.min.time(),
                    timezone.utc,
                ) + timedelta(hours=17)

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            record_full_market_day(
                repository,
                date(2026, 11, 27),
                bucket_count=42,
            )

            report = QualityReportService(
                repository,
                calendar=EarlyCloseCalendar(),
            ).build()
            coverage = report.shadow_validation[
                "regular_session_data_coverage"
            ]["2026-11-27"]

            self.assertEqual(coverage["nominal_regular_5m_buckets"], 42)
            self.assertEqual(coverage["covered_5m_buckets"], 42)
            self.assertTrue(coverage["full_session_data_recovered"])

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
                    "trigger": "schedule",
                    "plan": {
                        "tasks": [
                            {"name": "market", "checkpoint_key": "market"},
                        ]
                    },
                    "succeeded": ["market"],
                    "details": {
                        "market": {
                            "task": "market",
                            "status": "success",
                            "duration_ms": 350,
                            "metadata": {
                                "providers": {
                                    "yahoo_market": {
                                        "status": "success",
                                        "latency_ms": 300,
                                    }
                                }
                            },
                        }
                    },
                },
            )

            report = QualityReportService(repository).build()

            self.assertTrue(report.passed)
            self.assertEqual(report.messages_checked, 1)
            self.assertEqual(report.runtime["runs_checked"], 1)
            self.assertEqual(report.runtime["trigger_counts"], {"schedule": 1})
            self.assertEqual(report.runtime["scheduler_status"], "degraded")
            self.assertEqual(report.runtime["schedule_runs_checked"], 1)
            self.assertEqual(report.runtime["max_schedule_start_delay_seconds"], 12 * 60)
            self.assertEqual(report.runtime["schedule_starts_over_10_minutes"], 1)
            self.assertEqual(report.runtime["max_schedule_interval_seconds"], 0)
            self.assertEqual(report.shadow_validation["status"], "observing")
            self.assertIn(
                "two_full_trading_days",
                report.shadow_validation["blocked_reasons"],
            )
            self.assertFalse(
                report.shadow_validation["advisories"][
                    "scheduler_cadence_within_slo"
                ]
            )
            self.assertEqual(report.runtime["planned_tasks"], {"market": 1})
            self.assertEqual(report.runtime["succeeded_tasks"], {"market": 1})
            self.assertEqual(
                report.runtime["task_execution"]["market"],
                {
                    "calls": 1,
                    "success": 1,
                    "failed": 0,
                    "max_ms": 350,
                    "average_ms": 350,
                },
            )
            self.assertEqual(
                report.runtime["provider_health"]["yahoo_market"]["average_ms"],
                300,
            )
            self.assertEqual(
                report.recent_messages[0]["fallback_text"],
                "$VRT 09/02 장 마감 브리프",
            )

    def test_push_runs_do_not_masquerade_as_scheduler_health(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            repository.start_run(
                "push-run",
                scheduled_at=NOW - timedelta(hours=3),
                started_at=NOW,
                gap_seconds=3 * 60 * 60,
            )
            repository.finish_run(
                "push-run",
                completed_at=NOW + timedelta(seconds=2),
                status="success",
                summary={"trigger": "push", "plan": {"tasks": []}},
            )

            report = QualityReportService(repository).build()

            self.assertEqual(report.runtime["trigger_counts"], {"push": 1})
            self.assertEqual(report.runtime["scheduler_status"], "unobserved")
            self.assertEqual(report.runtime["schedule_runs_checked"], 0)
            self.assertIsNone(report.runtime["latest_schedule_started_at"])
            self.assertEqual(report.shadow_validation["status"], "blocked")

    def test_scheduler_health_uses_real_same_session_run_intervals(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            for run_id, started_at in (
                ("schedule-1", NOW - timedelta(minutes=25)),
                ("schedule-2", NOW),
            ):
                repository.start_run(
                    run_id,
                    scheduled_at=started_at,
                    started_at=started_at,
                    gap_seconds=0,
                )
                repository.finish_run(
                    run_id,
                    completed_at=started_at + timedelta(seconds=2),
                    status="success",
                    summary={"trigger": "schedule", "plan": {"tasks": []}},
                )

            report = QualityReportService(repository).build()

            self.assertEqual(report.runtime["scheduler_status"], "degraded")
            self.assertEqual(report.runtime["max_schedule_interval_seconds"], 25 * 60)
            self.assertEqual(report.runtime["p95_schedule_interval_seconds"], 25 * 60)
            self.assertEqual(report.runtime["schedule_intervals_over_15_minutes"], 1)

    def test_three_consecutive_provider_failures_open_an_incident(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            for index in range(3):
                started_at = NOW - timedelta(minutes=(2 - index) * 5)
                run_id = f"failed-provider-{index}"
                repository.start_run(
                    run_id,
                    scheduled_at=started_at,
                    started_at=started_at,
                    gap_seconds=5 * 60,
                )
                repository.finish_run(
                    run_id,
                    completed_at=started_at + timedelta(seconds=2),
                    status="success",
                    summary={
                        "trigger": "schedule",
                        "plan": {"tasks": []},
                        "details": {
                            "news": {
                                "task": "news",
                                "status": "success",
                                "metadata": {
                                    "providers": {
                                        "yahoo": {
                                            "status": "failed",
                                            "latency_ms": 100,
                                        }
                                    }
                                },
                            }
                        },
                    },
                )

            report = QualityReportService(repository).build()

            self.assertEqual(report.runtime["operational_status"], "incident")
            self.assertEqual(
                report.runtime["provider_state"]["yahoo"]["consecutive_failures"],
                3,
            )
            self.assertEqual(
                report.runtime["incidents"][0]["type"],
                "provider_failure",
            )

    def test_two_complete_shadow_days_unlock_test_slack_readiness(self):
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
            record_full_market_day(repository, date(2026, 9, 1))
            record_full_market_day(repository, date(2026, 9, 2))
            for day in (1, 2):
                session_start = datetime(2026, 9, day, 13, 30, tzinfo=timezone.utc)
                for index in range(37):
                    started_at = session_start + timedelta(minutes=index * 10)
                    repository.start_run(
                        f"schedule-{day}-{index}",
                        scheduled_at=started_at,
                        started_at=started_at,
                        gap_seconds=0,
                    )
                    repository.finish_run(
                        f"schedule-{day}-{index}",
                        completed_at=started_at + timedelta(seconds=2),
                        status="success",
                        summary={
                            "trigger": "schedule",
                            "plan": {"tasks": []},
                            "details": {
                                "market": {
                                    "task": "market",
                                    "status": "success",
                                    "duration_ms": 10,
                                    "metadata": {
                                        "providers": {
                                            "yahoo_market": {
                                                "status": "success",
                                                "latency_ms": 8,
                                            }
                                        }
                                    },
                                }
                            },
                        },
                    )

            report = QualityReportService(repository).build()

            self.assertEqual(report.runtime["scheduler_status"], "healthy")
            self.assertEqual(report.runtime["median_schedule_interval_seconds"], 600)
            self.assertEqual(report.runtime["p95_schedule_interval_seconds"], 600)
            self.assertIn("yahoo_market", report.runtime["scheduled_provider_health"])
            self.assertEqual(report.shadow_validation["full_shadow_days"], 2)
            self.assertEqual(
                report.shadow_validation["regular_session_data_coverage"][
                    "2026-09-02"
                ]["covered_5m_buckets"],
                78,
            )
            self.assertEqual(report.shadow_validation["status"], "ready_for_test_slack")

    def test_full_bar_recovery_is_distinct_from_sparse_scheduler_polling(self):
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
            for trading_date in (date(2026, 9, 1), date(2026, 9, 2)):
                record_full_market_day(repository, trading_date)
                for suffix, started_at in (
                    ("open", datetime.combine(trading_date, datetime.min.time(), timezone.utc) + timedelta(hours=14)),
                    ("close", datetime.combine(trading_date, datetime.min.time(), timezone.utc) + timedelta(hours=19, minutes=45)),
                ):
                    run_id = f"{trading_date}-{suffix}"
                    repository.start_run(
                        run_id,
                        scheduled_at=started_at,
                        started_at=started_at,
                        gap_seconds=3 * 60 * 60,
                    )
                    repository.finish_run(
                        run_id,
                        completed_at=started_at + timedelta(seconds=2),
                        status="success",
                        summary={
                            "trigger": "schedule",
                            "plan": {
                                "tasks": [
                                    {"name": "market", "checkpoint_key": "market"}
                                ]
                            },
                            "succeeded": ["market"],
                            "details": {
                                "market": {
                                    "task": "market",
                                    "status": "success",
                                    "duration_ms": 10,
                                    "metadata": {
                                        "observed_frames": 78 if suffix == "close" else 1,
                                        "replayed_frames": 77 if suffix == "close" else 0,
                                        "source_age_seconds": 20,
                                        "providers": {
                                            "yahoo_market": {
                                                "status": "success",
                                                "latency_ms": 8,
                                            }
                                        },
                                    },
                                }
                            },
                        },
                    )

            report = QualityReportService(repository).build()

            self.assertEqual(report.runtime["scheduler_status"], "degraded")
            self.assertFalse(
                report.shadow_validation["advisories"][
                    "scheduler_cadence_within_slo"
                ]
            )
            self.assertEqual(report.shadow_validation["full_shadow_days"], 2)
            self.assertEqual(report.runtime["market_recovery"]["replayed_frames"], 154)
            self.assertEqual(
                report.runtime["market_recovery"]["max_recovery_span_minutes"],
                385,
            )
            self.assertEqual(report.shadow_validation["status"], "ready_for_test_slack")

    def test_product_quality_detects_filtered_and_reconciled_evidence_alerts(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            record_full_market_day(repository, date(2026, 9, 1))
            record_full_market_day(repository, date(2026, 9, 2))
            rows = (
                ("valid", "https://example.com/valid", "analyzed", "", "event-valid", 1),
                ("filtered", "https://example.com/filtered", "filtered", "low value", "event-filtered", None),
                ("duplicate", "https://example.com/duplicate", "analyzed", "", "event-valid", 1),
                ("irrelevant", "https://example.com/irrelevant", "analyzed", "irrelevant", "event-irrelevant", 0),
            )
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                for candidate_id, source_url, status, reason, cluster_key, relevant in rows:
                    analysis_json = (
                        '{"relevant":true,"event_type":"acquisition",'
                        '"company_directness":true,"new_fact":true,'
                        '"materiality":"high","source_tier":"secondary",'
                        '"alert_worthy":true,"confidence":"high"}'
                        if relevant == 1
                        else '{"relevant":false}'
                        if relevant == 0
                        else "{}"
                    )
                    connection.execute(
                        "INSERT INTO evidence_candidates "
                        "(candidate_id, ticker, source_kind, headline, source_name, "
                        "source_url, published_at, cluster_key, status, status_reason, "
                        "analysis_json, first_seen_at, last_seen_at) "
                        "VALUES (?, 'VRT', 'news', ?, 'Source', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            candidate_id,
                            candidate_id,
                            source_url,
                            NOW.isoformat(),
                            cluster_key,
                            status,
                            reason,
                            analysis_json,
                            NOW.isoformat(),
                            NOW.isoformat(),
                        ),
                    )
            for candidate_id, source_url, _status, _reason, cluster_key, _relevant in rows:
                event_key = "event-old-duplicate" if candidate_id == "duplicate" else cluster_key
                repository.record_alert(
                    AlertRecord(
                        event_key=event_key,
                        ticker="VRT",
                        alert_type="catalyst",
                        created_at=NOW - timedelta(minutes=len(candidate_id)),
                        payload=valid_catalyst_payload(source_url, candidate_id),
                    ),
                    enqueue=False,
                )

            report = QualityReportService(repository).build()
            evidence = report.product_quality["evidence_alerts"]

            self.assertEqual(report.product_quality["status"], "needs_improvement")
            self.assertEqual(evidence["assessed"], 4)
            self.assertEqual(evidence["currently_valid"], 1)
            self.assertEqual(evidence["retrospectively_filtered"], 1)
            self.assertEqual(evidence["duplicate_cluster_reconciled"], 1)
            self.assertEqual(evidence["no_longer_relevant"], 1)
            self.assertEqual(evidence["meaningful_rate_percent"], 25.0)
            self.assertEqual(evidence["duplicate_rate_percent"], 25.0)
            self.assertEqual(evidence["recent_semantic_violations"], 3)
            self.assertEqual(
                report.product_quality["alert_load"]["days_over_target"],
                1,
            )
            self.assertIn(
                "recent_evidence_alerts_clean",
                report.shadow_validation["blocked_reasons"],
            )

    def test_product_quality_rejects_briefing_only_independent_alert(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            record_full_market_day(repository, date(2026, 9, 1))
            record_full_market_day(repository, date(2026, 9, 2))
            source_url = "https://example.com/capacity"
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                connection.execute(
                    "INSERT INTO evidence_candidates "
                    "(candidate_id, ticker, source_kind, headline, source_name, "
                    "source_url, published_at, cluster_key, status, analysis_json, "
                    "first_seen_at, last_seen_at) "
                    "VALUES ('capacity', 'VRT', 'news', 'capacity', 'Source', ?, ?, "
                    "'event-capacity', 'analyzed', ?, ?, ?)",
                    (
                        source_url,
                        NOW.isoformat(),
                        '{"relevant":true,"event_type":"capacity",'
                        '"company_directness":true,"new_fact":true,'
                        '"materiality":"medium","source_tier":"secondary",'
                        '"alert_worthy":false,"confidence":"high"}',
                        NOW.isoformat(),
                        NOW.isoformat(),
                    ),
                )
            repository.record_alert(
                AlertRecord(
                    event_key="event-capacity",
                    ticker="VRT",
                    alert_type="catalyst",
                    created_at=NOW,
                    payload=valid_catalyst_payload(source_url, "capacity"),
                ),
                enqueue=False,
            )

            report = QualityReportService(repository).build()
            evidence = report.product_quality["evidence_alerts"]

            self.assertEqual(evidence["briefing_only_alert"], 1)
            self.assertEqual(evidence["currently_valid"], 0)
            self.assertEqual(evidence["meaningful_rate_percent"], 0.0)
            self.assertEqual(evidence["recent_semantic_violations"], 1)
            self.assertEqual(
                report.product_quality["evidence_qualification"]["by_disposition"],
                {"briefing": 1},
            )

    def test_quality_report_attributes_alerts_to_current_build(self):
        with tempfile.TemporaryDirectory() as directory:
            build_sha = "b" * 40
            recorded_at = NOW + timedelta(seconds=30)
            repository = SQLiteMonitorRepository(
                Path(directory) / "monitor.db",
                build_sha=build_sha,
                workflow_name="Monitor V2 Runtime Shadow",
                clock=lambda: recorded_at,
            )
            repository.start_run(
                "run-99",
                scheduled_at=NOW,
                started_at=NOW + timedelta(seconds=5),
                gap_seconds=0,
            )
            source_url = "https://example.com/acquisition"
            with closing(sqlite3.connect(repository.path)) as connection, connection:
                connection.execute(
                    "INSERT INTO evidence_candidates "
                    "(candidate_id, ticker, source_kind, headline, source_name, "
                    "source_url, published_at, cluster_key, status, analysis_json, "
                    "first_seen_at, last_seen_at) "
                    "VALUES ('acquisition', 'VRT', 'news', 'acquisition', 'Source', "
                    "?, ?, 'event-acquisition', 'analyzed', ?, ?, ?)",
                    (
                        source_url,
                        NOW.isoformat(),
                        '{"relevant":true,"event_type":"acquisition",'
                        '"company_directness":true,"new_fact":true,'
                        '"materiality":"high","source_tier":"secondary",'
                        '"alert_worthy":true,"confidence":"high"}',
                        NOW.isoformat(),
                        NOW.isoformat(),
                    ),
                )
            repository.record_alert(
                AlertRecord(
                    event_key="event-acquisition",
                    ticker="VRT",
                    alert_type="catalyst",
                    created_at=NOW,
                    payload=valid_catalyst_payload(source_url, "acquisition"),
                ),
                enqueue=False,
            )
            repository.finish_run(
                "run-99",
                completed_at=recorded_at,
                status="success",
                summary={"trigger": "push", "plan": {"tasks": []}},
            )

            report = QualityReportService(repository).build()
            stored = repository.recent_alerts()[0]
            build_quality = report.product_quality["evidence_qualification"][
                "by_build"
            ][build_sha]

            self.assertEqual(stored.recorded_at, recorded_at)
            self.assertEqual(stored.build_sha, build_sha)
            self.assertEqual(stored.run_id, "run-99")
            self.assertEqual(
                report.runtime["build_provenance"]["current_build_sha"],
                build_sha,
            )
            self.assertEqual(
                report.runtime["build_provenance"]["by_build"][build_sha][
                    "workflows"
                ],
                ["Monitor V2 Runtime Shadow"],
            )
            self.assertEqual(build_quality["alerts_checked"], 1)
            self.assertEqual(build_quality["semantic_violations"], 0)
            self.assertEqual(build_quality["meaningful_rate_percent"], 100.0)

    def test_new_build_cannot_borrow_old_build_release_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            old_sha = "a" * 40
            new_sha = "b" * 40
            old_repository = SQLiteMonitorRepository(
                path,
                build_sha=old_sha,
                run_id="old-run",
            )
            record_full_market_day(old_repository, date(2026, 9, 1))
            record_full_market_day(old_repository, date(2026, 9, 2))
            old_repository.start_run(
                "old-run",
                scheduled_at=NOW,
                started_at=NOW,
                gap_seconds=0,
            )
            old_repository.finish_run(
                "old-run",
                completed_at=NOW + timedelta(seconds=2),
                status="success",
                summary={"trigger": "schedule", "plan": {"tasks": []}},
            )

            new_started_at = NOW + timedelta(days=1)
            new_repository = SQLiteMonitorRepository(
                path,
                build_sha=new_sha,
                workflow_name="Monitor V2 Runtime Shadow",
                run_id="new-run",
            )
            new_repository.start_run(
                "new-run",
                scheduled_at=new_started_at,
                started_at=new_started_at,
                gap_seconds=0,
            )
            new_repository.record_alert(
                AlertRecord(
                    event_key="VRT:2026-09-03:close",
                    ticker="VRT",
                    alert_type="daily_close",
                    created_at=new_started_at,
                    payload=valid_close_payload(),
                ),
                enqueue=False,
            )
            new_repository.finish_run(
                "new-run",
                completed_at=new_started_at + timedelta(seconds=2),
                status="success",
                summary={
                    "trigger": "schedule",
                    "plan": {"tasks": []},
                    "details": {
                        "market": {
                            "task": "market",
                            "status": "success",
                            "duration_ms": 10,
                            "metadata": {
                                "providers": {
                                    "yahoo_market": {
                                        "status": "success",
                                        "latency_ms": 8,
                                    }
                                }
                            },
                        }
                    },
                },
            )

            blocked = QualityReportService(new_repository).build()

            self.assertEqual(
                blocked.runtime["build_provenance"]["current_build_sha"],
                new_sha,
            )
            self.assertEqual(
                len(blocked.shadow_validation["regular_session_data_coverage"]),
                2,
            )
            self.assertEqual(
                blocked.shadow_validation["current_build_data_coverage"],
                {},
            )
            self.assertEqual(blocked.shadow_validation["full_shadow_days"], 0)
            self.assertFalse(
                blocked.shadow_validation["gates"]["two_full_trading_days"]
            )
            self.assertEqual(blocked.shadow_validation["status"], "observing")

            record_full_market_day(new_repository, date(2026, 9, 3))
            record_full_market_day(new_repository, date(2026, 9, 4))
            ready = QualityReportService(new_repository).build()

            self.assertEqual(ready.shadow_validation["full_shadow_days"], 2)
            self.assertEqual(
                sorted(ready.shadow_validation["current_build_data_coverage"]),
                ["2026-09-03", "2026-09-04"],
            )
            self.assertTrue(all(ready.shadow_validation["gates"].values()))
            self.assertEqual(
                ready.shadow_validation["status"],
                "ready_for_test_slack",
            )


if __name__ == "__main__":
    unittest.main()
