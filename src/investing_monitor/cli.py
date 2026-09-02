from __future__ import annotations

import argparse
import json
import os
import sqlite3
import uuid
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
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
from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION, SQLiteMonitorRepository
from investing_monitor.adapters.yahoo_market_data import (
    YahooChartClient,
    YahooMarketDataAdapter,
    YahooQuoteClient,
)
from investing_monitor.adapters.yahoo_news import YahooArticleTextClient, YahooNewsAdapter
from investing_monitor.application.briefs import CloseBriefService
from investing_monitor.application.evidence import EvidenceIngestionService
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.application.sec_monitor import SecMonitorService
from investing_monitor.presentation.evidence_messages import build_evidence_message
from investing_monitor.runtime.tick import TickPlanner, TickRunner, TickTask


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
    market_tick.add_argument("--config", default="monitor_config.md")
    market_tick.add_argument("--now", default="", help="ISO-8601 timestamp, defaults to now")
    market_tick.add_argument("--scheduled-at", default="")
    market_tick.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))

    shadow_tick = subparsers.add_parser(
        "shadow-tick",
        help="run market, evidence, and close monitors without delivering Slack",
    )
    shadow_tick.add_argument("--config", default="monitor_config.md")
    shadow_tick.add_argument("--now", default="", help="ISO-8601 timestamp, defaults to now")
    shadow_tick.add_argument("--scheduled-at", default="")
    shadow_tick.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))

    subparsers.add_parser("status", help="show persisted runtime status")

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

    if args.command in {"market-tick", "shadow-tick"}:
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
        suppressed_deliveries = repository.suppress_pending_deliveries(
            now,
            "v2 shadow runtime does not deliver Slack",
        )
        service = MarketCycleService(repository, enqueue_alerts=False)
        market_result: dict[str, object] = {}
        evidence_result: dict[str, object] = {}
        close_result: dict[str, object] = {}

        def handle_market(_task):
            cycle = adapter.fetch_cycle(
                now,
                last_observed_at=repository.latest_market_observation_at(profile.ticker),
            )
            report = service.process(
                cycle,
                repository.recent_catalysts(
                    profile.ticker,
                    now - timedelta(hours=24),
                    limit=2,
                ),
            )
            market_result.update(report.as_dict())
            market_result["source_age_seconds"] = cycle.source_age_seconds
            return {
                "observed_frames": report.observed_frames,
                "replayed_frames": report.replayed_frames,
                "inserted_events": len(report.inserted_event_keys),
                "source_age_seconds": cycle.source_age_seconds,
            }

        handlers = {TickTask.MARKET: handle_market}
        enabled_tasks = {TickTask.MARKET}
        if args.command == "shadow-tick":
            evidence_profile = load_evidence_profile(args.config)
            ingestion = EvidenceIngestionService(
                repository,
                evidence_profile,
                AnthropicEvidenceAnalyzer(os.environ.get("ANTHROPIC_API_KEY", "")),
                article_text=YahooArticleTextClient(),
                filing_text=SecFilingTextClient(evidence_profile.sec_contact),
                alert_builder=build_evidence_message,
                enqueue_alerts=False,
            )
            yahoo_news = YahooNewsAdapter()
            investor_relations = InvestorRelationsFeedAdapter()

            def handle_news(_task):
                candidates = []
                source_errors = {}
                for source_name, source in (
                    ("yahoo", yahoo_news),
                    ("ir", investor_relations),
                ):
                    try:
                        candidates.extend(source.fetch(evidence_profile))
                    except Exception as exc:
                        source_errors[source_name] = str(exc)
                if not candidates:
                    raise RuntimeError(
                        "all news sources failed: "
                        + "; ".join(
                            f"{name}={error}" for name, error in source_errors.items()
                        )
                    )
                report = ingestion.ingest(candidates, now)
                evidence_result["news"] = {
                    **report.as_dict(),
                    "source_errors": source_errors,
                }
                return evidence_result["news"]

            sec_service = SecMonitorService(
                repository,
                evidence_profile,
                ResilientSecFilingsAdapter(),
                ingestion,
            )

            def handle_sec(_task):
                report = sec_service.poll(now)
                evidence_result["sec"] = report.as_dict()
                return evidence_result["sec"]

            close_service = CloseBriefService(repository, enqueue_alerts=False)

            def handle_close(task):
                close_date = date.fromisoformat(task.checkpoint_key.rsplit(":", 1)[-1])
                report = close_service.process(
                    profile.ticker,
                    close_date,
                    trading_open_at=calendar.regular_open(close_date),
                    created_at=now,
                )
                close_result.update(report.as_dict())
                return {
                    "event_key": report.event_key,
                    "inserted": report.inserted,
                    "catalyst_count": report.catalyst_count,
                }

            handlers.update(
                {
                    TickTask.NEWS: handle_news,
                    TickTask.SEC: handle_sec,
                    TickTask.CLOSE: handle_close,
                }
            )
            enabled_tasks.update({TickTask.NEWS, TickTask.SEC, TickTask.CLOSE})

        run_id = args.run_id or f"manual-{uuid.uuid4()}"
        execution = TickRunner(
            repository,
            handlers,
            planner=TickPlanner(
                calendar=calendar,
                enabled_tasks=enabled_tasks,
            ),
            clock=lambda: now,
        ).run(run_id, scheduled_at=scheduled_at, started_at=now)
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
        if args.command == "shadow-tick":
            payload["evidence"] = evidence_result
            payload["close"] = close_result
        _emit(payload, args.summary_file)
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
                }
                for run in repository.recent_runs()
            ],
        }
        return _emit(payload, args.summary_file)

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


def _state_store(args: argparse.Namespace) -> GitStateBranchStore:
    return GitStateBranchStore(
        args.repository,
        remote=args.remote,
        branch=args.branch,
    )


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


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _emit(payload: dict[str, object], summary_file: str) -> int:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if summary_file:
        path = Path(summary_file)
        with path.open("a", encoding="utf-8") as handle:
            handle.write("## Investing Monitor v2\n\n```json\n")
            handle.write(rendered)
            handle.write("\n```\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
