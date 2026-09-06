from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import sqlite3
import tempfile
import uuid
from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Sequence

from investing_monitor.adapters.git_state_branch import GitStateBranchStore
from investing_monitor.adapters.anthropic_evidence import AnthropicEvidenceAnalyzer
from investing_monitor.adapters.config import (
    load_evidence_profile,
    load_instrument_profile,
)
from investing_monitor.adapters.evidence_feeds import InvestorRelationsFeedAdapter
from investing_monitor.adapters.exchange_calendar import XNYSCalendar
from investing_monitor.adapters.sec_filings import (
    ResilientSecFilingsAdapter,
    SecFilingTextClient,
)
from investing_monitor.adapters.slack import SlackWebhookNotifier
from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION, SQLiteMonitorRepository
from investing_monitor.adapters.yahoo_market_data import (
    YahooChartClient,
    YahooMarketDataAdapter,
    YahooQuoteClient,
)
from investing_monitor.adapters.yahoo_news import YahooArticleTextClient, YahooNewsAdapter
from investing_monitor.application.briefs import (
    CloseBriefService,
    WeeklyBriefService,
    WeeklyBriefUnavailable,
)
from investing_monitor.application.evidence import EvidenceIngestionService
from investing_monitor.application.monitor import MarketCycleService, OutboxDeliveryService
from investing_monitor.application.quality import QualityReportService
from investing_monitor.application.replay import MarketReplayLab
from investing_monitor.application.sec_monitor import SecMonitorService
from investing_monitor.presentation.evidence_messages import (
    build_evidence_message,
    build_move_followup_message,
)
from investing_monitor.presentation.operations import (
    build_quality_summary,
    build_tick_summary,
    quality_annotations,
)
from investing_monitor.presentation.previews import PREVIEW_KINDS, build_preview_message
from investing_monitor.presentation.quality import audit_message
from investing_monitor.ports.repository import AlertRecord
from investing_monitor.runtime.tick import (
    NEW_YORK,
    TaskExecutionError,
    TickPlanner,
    TickRunner,
    TickTask,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="investing-monitor")
    parser.add_argument(
        "--db",
        default=os.environ.get("MONITOR_DB_PATH", ".runtime/monitor.db"),
        help="runtime SQLite path",
    )
    parser.add_argument(
        "--summary-file",
        default=os.environ.get("GITHUB_STEP_SUMMARY", ""),
        help="optional GitHub Actions Job Summary path",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="show due tasks without executing providers")
    plan.add_argument("--now", default="", help="ISO-8601 timestamp, defaults to now")

    market_tick = subparsers.add_parser(
        "market-tick",
        help="run the Stage 2 Yahoo market monitor without delivering Slack",
    )
    _add_tick_arguments(market_tick)

    shadow_tick = subparsers.add_parser(
        "shadow-tick",
        help="run market, evidence, and close monitors without delivering Slack",
    )
    _add_tick_arguments(shadow_tick)

    production_tick = subparsers.add_parser(
        "production-tick",
        help="run the unified monitor and deliver qualified outbox messages to Slack",
    )
    _add_tick_arguments(production_tick)
    _add_git_state_arguments(production_tick)

    slack_canary = subparsers.add_parser(
        "slack-canary",
        help="send one labeled stored v2 message through the production Slack adapter",
    )
    slack_canary.add_argument("--config", default="monitor_config.md")
    slack_canary.add_argument(
        "--run-id",
        default=os.environ.get("GITHUB_RUN_ID", ""),
    )
    _add_git_state_arguments(slack_canary)

    slack_preview = subparsers.add_parser(
        "slack-preview",
        help="send one labeled fixture message through the production Slack adapter",
    )
    slack_preview.add_argument("--config", default="monitor_config.md")
    slack_preview.add_argument("--kind", choices=PREVIEW_KINDS, default="move-up")
    slack_preview.add_argument(
        "--run-id",
        default=os.environ.get("GITHUB_RUN_ID", ""),
    )
    _add_git_state_arguments(slack_preview)

    replay = subparsers.add_parser(
        "replay-market",
        help="replay completed trading days in an isolated quality lab",
    )
    replay.add_argument("--config", default="monitor_config.md")
    replay.add_argument("--days", type=int, default=3)
    replay.add_argument("--end-date", default="", help="YYYY-MM-DD, defaults to latest close")

    subparsers.add_parser("status", help="show persisted runtime status")

    quality = subparsers.add_parser(
        "quality-report",
        help="audit recent messages and runtime execution quality",
    )
    quality.add_argument("--limit", type=int, default=100)

    doctor = subparsers.add_parser("doctor", help="validate the GitHub runtime foundation")
    doctor.add_argument("--require-secrets", action="store_true")

    restore = subparsers.add_parser("restore-state", help="restore the runtime-state branch")
    _add_git_state_arguments(restore)

    checkpoint = subparsers.add_parser(
        "checkpoint-state",
        help="write a rolling snapshot to the runtime-state branch",
    )
    _add_git_state_arguments(checkpoint)
    checkpoint.add_argument(
        "--run-id",
        default=os.environ.get("GITHUB_RUN_ID", "manual"),
    )
    return parser


def _add_git_state_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", default=".")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="runtime-state")


def _add_tick_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="monitor_config.md")
    parser.add_argument(
        "--now",
        default="",
        help="ISO-8601 timestamp, defaults to now",
    )
    parser.add_argument("--scheduled-at", default="")
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    parser.add_argument(
        "--trigger",
        default=os.environ.get("GITHUB_EVENT_NAME", "manual"),
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database_path = Path(args.db)

    if args.command == "restore-state":
        store = _state_store(args)
        result = store.restore(database_path)
        if not result.restored:
            SQLiteMonitorRepository(database_path)
        payload = {
            "command": args.command,
            "restored": result.restored,
            "commit_sha": result.commit_sha,
            "database_sha256": result.database_sha256,
        }
        return _emit(payload, args.summary_file)

    repository = SQLiteMonitorRepository(database_path)

    if args.command == "checkpoint-state":
        result = _state_store(args).checkpoint(database_path, run_id=args.run_id)
        payload = {
            "command": args.command,
            "commit_sha": result.commit_sha,
            "previous_commit_sha": result.previous_commit_sha,
            "database_sha256": result.database_sha256,
        }
        return _emit(payload, args.summary_file)

    if args.command == "slack-canary":
        run_id = args.run_id or f"manual-{uuid.uuid4()}"
        profile = load_instrument_profile(args.config)
        source = _latest_valid_product_alert(repository)
        payload = _build_slack_canary(source.payload)
        event_key = f"delivery-canary:{run_id}"
        delivery, checkpoint_count = _deliver_test_message(
            repository,
            database_path,
            args,
            event_key=event_key,
            ticker=profile.ticker,
            payload=payload,
            run_id=run_id,
        )
        _emit(
            {
                "command": args.command,
                "event_key": event_key,
                "source_event_key": source.event_key,
                "delivered": delivery.delivered,
                "delivery": delivery.as_dict(),
                "remote_checkpoints": checkpoint_count,
            },
            args.summary_file,
        )
        return 0 if delivery.delivered == 1 else 1

    if args.command == "slack-preview":
        run_id = args.run_id or f"manual-{uuid.uuid4()}"
        profile = load_instrument_profile(args.config)
        payload = build_preview_message(
            args.kind,
            ticker=profile.ticker,
            benchmark=profile.benchmark,
            peers=profile.peers,
            now=datetime.now(timezone.utc),
        )
        event_key = f"delivery-preview:{args.kind}:{run_id}"
        delivery, checkpoint_count = _deliver_test_message(
            repository,
            database_path,
            args,
            event_key=event_key,
            ticker=profile.ticker,
            payload=payload,
            run_id=run_id,
        )
        _emit(
            {
                "command": args.command,
                "kind": args.kind,
                "event_key": event_key,
                "delivery": delivery.as_dict(),
                "remote_checkpoints": checkpoint_count,
            },
            args.summary_file,
        )
        return 0 if delivery.delivered == 1 else 1

    if args.command == "replay-market":
        now = datetime.now(timezone.utc)
        profile = load_instrument_profile(args.config)
        calendar = XNYSCalendar()
        if args.end_date:
            end_date = date.fromisoformat(args.end_date)
            if not calendar.is_trading_day(end_date):
                raise ValueError(f"{end_date} is not an XNYS trading day")
        else:
            end_date = _latest_completed_trading_date(now, calendar)
        replay_dates = [end_date]
        for _ in range(max(1, args.days) - 1):
            replay_dates.append(calendar.previous_trading_day(replay_dates[-1]))
        replay_dates.reverse()

        with tempfile.TemporaryDirectory(prefix="investing-monitor-replay-") as directory:
            replay_repository = SQLiteMonitorRepository(Path(directory) / "replay.db")
            replay_adapter = YahooMarketDataAdapter(
                YahooChartClient(),
                calendar,
                profile,
            )
            lab = MarketReplayLab(
                replay_adapter,
                calendar,
                MarketCycleService(replay_repository, enqueue_alerts=False),
            )
            reports = [lab.replay_day(value) for value in replay_dates]
        passed = all(report.quality_passed for report in reports)
        _emit(
            {
                "command": args.command,
                "profile": {
                    "ticker": profile.ticker,
                    "benchmark": profile.benchmark,
                    "peers": list(profile.peers),
                },
                "passed": passed,
                "days": [report.as_dict() for report in reports],
            },
            args.summary_file,
        )
        return 0 if passed else 1

    if args.command in {"market-tick", "shadow-tick", "production-tick"}:
        production = args.command == "production-tick"
        full_monitor = args.command in {"shadow-tick", "production-tick"}
        if production and os.environ.get("V2_PRODUCTION_ENABLED", "").lower() != "true":
            raise RuntimeError("production delivery requires V2_PRODUCTION_ENABLED=true")
        now = _parse_timestamp(args.now) if args.now else datetime.now(timezone.utc)
        scheduled_at = (
            _parse_timestamp(args.scheduled_at)
            if args.scheduled_at
            else _nominal_market_tick(now)
        )
        profile = load_instrument_profile(args.config)
        calendar = XNYSCalendar()
        adapter = YahooMarketDataAdapter(
            YahooChartClient(),
            calendar,
            profile,
            quote_client=YahooQuoteClient(),
        )
        suppressed_deliveries = (
            0
            if production
            else repository.suppress_pending_deliveries(
                now,
                "v2 shadow runtime does not deliver Slack",
            )
        )
        service = MarketCycleService(repository, enqueue_alerts=production)
        sensitivity = repository.load_market_sensitivity(profile.ticker)
        if sensitivity is not None and (
            sensitivity.benchmark_symbol != profile.benchmark
            or sensitivity.peer_symbols != profile.peers
        ):
            sensitivity = None
        market_result: dict[str, object] = {}
        evidence_result: dict[str, object] = {}
        close_result: dict[str, object] = {}
        weekly_result: dict[str, object] = {}

        def handle_market(_task):
            nonlocal sensitivity
            provider_started = monotonic()
            try:
                cycle = adapter.fetch_cycle(
                    now,
                    last_observed_at=repository.latest_market_observation_at(profile.ticker),
                )
            except Exception as exc:
                raise TaskExecutionError(
                    str(exc),
                    metadata={
                        "providers": {
                            "yahoo_market": {
                                "status": "failed",
                                "latency_ms": int(
                                    (monotonic() - provider_started) * 1_000
                                ),
                            }
                        }
                    },
                ) from exc
            sensitivity_status = "cached"
            sensitivity_error = ""
            if (
                sensitivity is None
                or now - sensitivity.calculated_at.astimezone(timezone.utc)
                >= timedelta(days=7)
            ):
                try:
                    refreshed = adapter.fetch_sensitivity(now)
                except Exception as exc:
                    sensitivity_error = str(exc)
                    if (
                        sensitivity is None
                        or now - sensitivity.calculated_at.astimezone(timezone.utc)
                        > timedelta(days=30)
                    ):
                        sensitivity = None
                        sensitivity_status = "unavailable"
                    else:
                        sensitivity_status = "stale-cache"
                else:
                    repository.save_market_sensitivity(refreshed)
                    sensitivity = refreshed
                    sensitivity_status = "refreshed"
            report = service.process(
                cycle,
                repository.recent_catalysts(
                    profile.ticker,
                    now - timedelta(hours=24),
                    limit=20,
                ),
                detected_at=now,
                sensitivity=sensitivity,
            )
            market_result.update(report.as_dict())
            market_result["source_age_seconds"] = cycle.source_age_seconds
            market_result["sensitivity_model"] = {
                "status": sensitivity_status,
                "samples": (
                    min(
                        sensitivity.benchmark_samples,
                        sensitivity.peer_samples,
                    )
                    if sensitivity
                    else 0
                ),
                "error": sensitivity_error,
            }
            return {
                "observed_frames": report.observed_frames,
                "replayed_frames": report.replayed_frames,
                "inserted_events": len(report.inserted_event_keys),
                "delayed_events": len(report.delayed_event_keys),
                "max_detection_delay_seconds": report.max_detection_delay_seconds,
                "source_age_seconds": cycle.source_age_seconds,
                "sensitivity_model": market_result["sensitivity_model"],
                "providers": {
                    "yahoo_market": {
                        "status": "success",
                        "latency_ms": int((monotonic() - provider_started) * 1_000),
                    }
                },
            }

        handlers = {TickTask.MARKET: handle_market}
        enabled_tasks = {TickTask.MARKET}
        if full_monitor:
            evidence_profile = load_evidence_profile(args.config)
            ingestion = EvidenceIngestionService(
                repository,
                evidence_profile,
                AnthropicEvidenceAnalyzer(os.environ.get("ANTHROPIC_API_KEY", "")),
                article_text=YahooArticleTextClient(),
                filing_text=SecFilingTextClient(evidence_profile.sec_contact),
                alert_builder=build_evidence_message,
                move_followup_builder=build_move_followup_message,
                enqueue_alerts=production,
            )
            yahoo_news = YahooNewsAdapter()
            investor_relations = InvestorRelationsFeedAdapter()

            def handle_news(_task):
                candidates = []
                source_errors = {}
                provider_health = {}
                for source_name, source in (
                    ("yahoo", yahoo_news),
                    ("ir", investor_relations),
                ):
                    source_started = monotonic()
                    try:
                        fetched = source.fetch(evidence_profile)
                        candidates.extend(fetched)
                        provider_health[source_name] = {
                            "status": "success",
                            "latency_ms": int(
                                (monotonic() - source_started) * 1_000
                            ),
                            "items": len(fetched),
                        }
                    except Exception as exc:
                        source_errors[source_name] = str(exc)
                        provider_health[source_name] = {
                            "status": "failed",
                            "latency_ms": int(
                                (monotonic() - source_started) * 1_000
                            ),
                        }
                if len(source_errors) == 2:
                    raise TaskExecutionError(
                        "all news sources failed: "
                        + "; ".join(
                            f"{name}={error}" for name, error in source_errors.items()
                        ),
                        metadata={"providers": provider_health},
                    )
                analysis_started = monotonic()
                report = ingestion.ingest(candidates, now)
                provider_health["evidence_pipeline"] = {
                    "status": "success" if report.failed == 0 else "degraded",
                    "latency_ms": int((monotonic() - analysis_started) * 1_000),
                    "analyzed": report.analyzed,
                    "failed": report.failed,
                }
                evidence_result["news"] = {
                    **report.as_dict(),
                    "source_errors": source_errors,
                    "providers": provider_health,
                }
                return evidence_result["news"]

            sec_service = SecMonitorService(
                repository,
                evidence_profile,
                ResilientSecFilingsAdapter(),
                ingestion,
            )

            def handle_sec(_task):
                provider_started = monotonic()
                try:
                    report = sec_service.poll(now)
                except Exception as exc:
                    raise TaskExecutionError(
                        str(exc),
                        metadata={
                            "providers": {
                                "sec": {
                                    "status": "failed",
                                    "latency_ms": int(
                                        (monotonic() - provider_started) * 1_000
                                    ),
                                }
                            }
                        },
                    ) from exc
                evidence_result["sec"] = {
                    **report.as_dict(),
                    "providers": {
                        f"sec_{report.provider}": {
                            "status": "recovered" if report.recovered else "success",
                            "latency_ms": int(
                                (monotonic() - provider_started) * 1_000
                            ),
                        }
                    },
                }
                return evidence_result["sec"]

            close_service = CloseBriefService(repository, enqueue_alerts=production)
            weekly_service = WeeklyBriefService(repository, enqueue_alerts=production)

            def handle_close(task):
                close_date = date.fromisoformat(task.checkpoint_key.rsplit(":", 1)[-1])
                report = close_service.process(
                    profile.ticker,
                    close_date,
                    trading_open_at=calendar.regular_open(close_date),
                    created_at=now,
                    sensitivity=sensitivity,
                )
                close_result.update(report.as_dict())
                return {
                    "event_key": report.event_key,
                    "inserted": report.inserted,
                    "catalyst_count": report.catalyst_count,
                }

            def handle_weekly(_task):
                ny_date = now.astimezone(NEW_YORK).date()
                period_end = calendar.previous_trading_day(ny_date)
                period_start = period_end - timedelta(days=period_end.weekday())
                evidence_since = datetime.combine(
                    period_start,
                    time.min,
                    NEW_YORK,
                ).astimezone(timezone.utc)
                try:
                    report = weekly_service.process(
                        profile.ticker,
                        period_start,
                        period_end,
                        evidence_since=evidence_since,
                        created_at=now,
                    )
                except WeeklyBriefUnavailable as exc:
                    weekly_result.update(
                        {
                            "period_start": period_start.isoformat(),
                            "period_end": period_end.isoformat(),
                            "skipped": str(exc),
                        }
                    )
                    return weekly_result
                weekly_result.update(report.as_dict())
                return {
                    "event_key": report.event_key,
                    "inserted": report.inserted,
                    "market_sessions": report.market_sessions,
                    "strengthening_count": report.strengthening_count,
                    "risk_count": report.risk_count,
                    "upcoming_event_count": report.upcoming_event_count,
                }

            handlers.update(
                {
                    TickTask.NEWS: handle_news,
                    TickTask.SEC: handle_sec,
                    TickTask.CLOSE: handle_close,
                    TickTask.WEEKLY: handle_weekly,
                }
            )
            enabled_tasks.update(
                {TickTask.NEWS, TickTask.SEC, TickTask.CLOSE, TickTask.WEEKLY}
            )

        run_id = args.run_id or f"manual-{uuid.uuid4()}"
        delivery_result: dict[str, object] = {}
        if production:
            state_store = _state_store(args)
            delivery_checkpoints = []

            def checkpoint_delivery(reason: str) -> None:
                delivery_checkpoints.append(
                    state_store.checkpoint(
                        database_path,
                        run_id=f"{run_id}:{reason}",
                    )
                )

            delivery_service = OutboxDeliveryService(
                repository,
                SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", "")),
                checkpoint=checkpoint_delivery,
            )

            def handle_delivery(_task):
                started = monotonic()
                report = asyncio.run(delivery_service.deliver_pending())
                delivery_result.update(
                    {
                        **report.as_dict(),
                        "remote_checkpoints": len(delivery_checkpoints),
                    }
                )
                return {
                    **delivery_result,
                    "duration_ms": int((monotonic() - started) * 1_000),
                }

            handlers[TickTask.DELIVERY] = handle_delivery
            enabled_tasks.add(TickTask.DELIVERY)
        execution = TickRunner(
            repository,
            handlers,
            planner=TickPlanner(
                calendar=calendar,
                enabled_tasks=enabled_tasks,
            ),
            clock=lambda: now,
        ).run(
            run_id,
            scheduled_at=scheduled_at,
            started_at=now,
            trigger=args.trigger,
        )
        payload = {
            "command": args.command,
            "profile": {
                "ticker": profile.ticker,
                "benchmark": profile.benchmark,
                "peers": list(profile.peers),
            },
            "schedule_delay_seconds": max(
                0,
                int((now - scheduled_at).total_seconds()),
            ),
            "suppressed_deliveries": suppressed_deliveries,
            "execution": execution.as_dict(),
            "market": market_result,
        }
        if full_monitor:
            payload["evidence"] = evidence_result
            payload["close"] = close_result
            payload["weekly"] = weekly_result
        if production:
            payload["delivery"] = delivery_result
        _emit(
            payload,
            args.summary_file,
            summary_markdown=build_tick_summary(payload),
        )
        return 0 if execution.status == "success" else 1

    if args.command == "plan":
        now = _parse_timestamp(args.now) if args.now else datetime.now(timezone.utc)
        plan = TickPlanner().plan(
            now,
            repository.task_checkpoints(),
            last_completed_run_at=repository.last_completed_run_at(),
        )
        return _emit({"command": args.command, **plan.as_dict()}, args.summary_file)

    if args.command == "status":
        checkpoints = repository.task_checkpoints()
        payload = {
            "command": args.command,
            "database": str(database_path),
            "last_completed_run_at": _iso(repository.last_completed_run_at()),
            "task_checkpoints": {
                key: {
                    "task_name": item.task_name,
                    "last_success_at": _iso(item.last_success_at),
                    "last_attempt_at": _iso(item.last_attempt_at),
                    "last_error": item.last_error,
                    "metadata": dict(item.metadata),
                }
                for key, item in sorted(checkpoints.items())
            },
            "recent_runs": [
                {
                    "run_id": run.run_id,
                    "scheduled_at": _iso(run.scheduled_at),
                    "started_at": _iso(run.started_at),
                    "completed_at": _iso(run.completed_at),
                    "status": run.status,
                    "gap_seconds": run.gap_seconds,
                    "build_sha": run.build_sha,
                    "workflow_name": run.workflow_name,
                }
                for run in repository.recent_runs()
            ],
        }
        return _emit(payload, args.summary_file)

    if args.command == "quality-report":
        report = QualityReportService(
            repository,
            calendar=XNYSCalendar(),
        ).build(limit=max(1, args.limit))
        payload = {"command": args.command, **report.as_dict()}
        for annotation in quality_annotations(payload):
            print(annotation)
        _emit(
            payload,
            args.summary_file,
            summary_markdown=build_quality_summary(payload),
        )
        return 0 if report.passed else 1

    if args.command == "doctor":
        with closing(sqlite3.connect(database_path)) as connection, connection:
            quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
            schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
        required_secrets = ("SLACK_WEBHOOK_URL", "ANTHROPIC_API_KEY")
        secret_presence = {name: bool(os.environ.get(name)) for name in required_secrets}
        checks = {
            "database_quick_check": quick_check,
            "schema_version": schema_version,
            "expected_schema_version": SCHEMA_VERSION,
            "secrets_present": secret_presence,
            "github_actions": bool(os.environ.get("GITHUB_ACTIONS")),
        }
        ok = quick_check == "ok" and schema_version == SCHEMA_VERSION
        if args.require_secrets:
            ok = ok and all(secret_presence.values())
        _emit({"command": args.command, "ok": ok, "checks": checks}, args.summary_file)
        return 0 if ok else 1

    raise AssertionError(f"unhandled command: {args.command}")


def _latest_valid_product_alert(repository: SQLiteMonitorRepository) -> AlertRecord:
    for alert in repository.recent_alerts(limit=100):
        if alert.alert_type == "delivery_canary":
            continue
        if audit_message(alert.alert_type, alert.payload).passed:
            return alert
    raise RuntimeError("no quality-approved v2 message is available for Slack canary")


def _build_slack_canary(source: dict) -> dict:
    payload = copy.deepcopy(source)
    fallback = str(payload.get("text") or "V2 Slack message")
    payload["text"] = f"[V2 검증] {fallback}"[:3_000]
    blocks = payload.get("blocks")
    if not isinstance(blocks, list) or not blocks:
        raise ValueError("canary source must contain Slack blocks")
    for block in blocks:
        if not isinstance(block, dict) or block.get("type") != "header":
            continue
        text = block.get("text")
        if isinstance(text, dict) and isinstance(text.get("text"), str):
            text["text"] = f"V2 검증 · {text['text']}"[:150]
            break
    blocks.insert(
        1,
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "전송 경로 검증 · 투자 신호가 아닙니다",
                }
            ],
        },
    )
    result = audit_message("delivery_canary", payload)
    if not result.passed:
        raise ValueError("invalid Slack canary: " + "; ".join(result.violations))
    return payload


def _state_store(args: argparse.Namespace) -> GitStateBranchStore:
    return GitStateBranchStore(
        args.repository,
        remote=args.remote,
        branch=args.branch,
    )


def _deliver_test_message(
    repository: SQLiteMonitorRepository,
    database_path: Path,
    args: argparse.Namespace,
    *,
    event_key: str,
    ticker: str,
    payload: dict,
    run_id: str,
):
    inserted = repository.record_alert(
        AlertRecord(
            event_key=event_key,
            ticker=ticker,
            alert_type="delivery_canary",
            created_at=datetime.now(timezone.utc),
            payload=payload,
        )
    )
    if not inserted:
        raise RuntimeError(f"test delivery already exists for run {run_id}")
    checkpoints = []
    state_store = _state_store(args)

    def checkpoint_delivery(reason: str) -> None:
        checkpoints.append(
            state_store.checkpoint(
                database_path,
                run_id=f"{run_id}:{reason}",
            )
        )

    report = asyncio.run(
        OutboxDeliveryService(
            repository,
            SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", "")),
            checkpoint=checkpoint_delivery,
        ).deliver_pending(event_key=event_key)
    )
    return report, len(checkpoints)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _nominal_market_tick(now: datetime) -> datetime:
    now = now.astimezone(timezone.utc)
    seconds = int(now.timestamp())
    scheduled_seconds = ((seconds - 120) // 300) * 300 + 120
    return datetime.fromtimestamp(scheduled_seconds, timezone.utc)


def _latest_completed_trading_date(now: datetime, calendar: XNYSCalendar) -> date:
    ny_now = now.astimezone(NEW_YORK)
    if (
        calendar.is_trading_day(ny_now.date())
        and now >= calendar.regular_close(ny_now.date())
    ):
        return ny_now.date()
    return calendar.previous_trading_day(ny_now.date())


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _emit(
    payload: dict[str, object],
    summary_file: str,
    *,
    summary_markdown: str = "",
) -> int:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if summary_file:
        path = Path(summary_file)
        with path.open("a", encoding="utf-8") as handle:
            if summary_markdown:
                handle.write(summary_markdown.rstrip())
                handle.write("\n\n")
            handle.write("## Investing Monitor v2\n\n```json\n")
            handle.write(rendered)
            handle.write("\n```\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
