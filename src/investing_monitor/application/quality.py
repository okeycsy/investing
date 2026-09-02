from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time
from math import ceil
from statistics import median
from zoneinfo import ZoneInfo

from investing_monitor.ports.repository import MonitorRepository
from investing_monitor.presentation.quality import audit_message


@dataclass(frozen=True)
class QualityReport:
    passed: bool
    messages_checked: int
    message_violations: tuple[dict[str, object], ...]
    recent_messages: tuple[dict[str, object], ...]
    runtime: dict[str, object]
    shadow_validation: dict[str, object]
    status_counts: dict[str, dict[str, int]]

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "messages_checked": self.messages_checked,
            "message_violations": list(self.message_violations),
            "recent_messages": list(self.recent_messages),
            "runtime": self.runtime,
            "shadow_validation": self.shadow_validation,
            "status_counts": self.status_counts,
        }


class QualityReportService:
    def __init__(self, repository: MonitorRepository) -> None:
        self.repository = repository

    def build(self, *, limit: int = 100) -> QualityReport:
        alerts = self.repository.recent_alerts(limit=limit)
        violations = []
        samples = []
        for alert in alerts:
            result = audit_message(alert.alert_type, alert.payload)
            if not result.passed:
                violations.append(
                    {
                        "event_key": alert.event_key,
                        "alert_type": alert.alert_type,
                        **result.as_dict(),
                    }
                )
            if len(samples) < 10:
                samples.append(
                    {
                        "event_key": alert.event_key,
                        "alert_type": alert.alert_type,
                        "created_at": alert.created_at.isoformat(),
                        "fallback_text": alert.payload.get("text", ""),
                        **result.as_dict(),
                    }
                )

        runs = self.repository.recent_runs(limit=limit)
        planned = Counter()
        succeeded = Counter()
        task_samples: dict[str, dict[str, int]] = {}
        provider_samples: dict[str, dict[str, int]] = {}
        scheduled_provider_samples: dict[str, dict[str, int]] = {}
        for run in runs:
            run_trigger = _run_trigger(run.summary)
            plan_rows = (run.summary.get("plan") or {}).get("tasks") or []
            task_names = {
                str(row.get("checkpoint_key") or ""): str(row.get("name") or "")
                for row in plan_rows
                if isinstance(row, dict)
            }
            planned.update(name for name in task_names.values() if name)
            succeeded.update(
                task_names[key]
                for key in run.summary.get("succeeded") or []
                if key in task_names
            )
            details = run.summary.get("details") or {}
            if not isinstance(details, dict):
                details = {}
            for detail in details.values():
                if not isinstance(detail, dict):
                    continue
                task_name = str(detail.get("task") or "unknown")
                task_status = str(detail.get("status") or "unknown")
                duration_ms = max(0, int(detail.get("duration_ms") or 0))
                task_sample = task_samples.setdefault(
                    task_name,
                    {"calls": 0, "success": 0, "failed": 0, "total_ms": 0, "max_ms": 0},
                )
                task_sample["calls"] += 1
                task_sample["success"] += int(task_status == "success")
                task_sample["failed"] += int(task_status == "failed")
                task_sample["total_ms"] += duration_ms
                task_sample["max_ms"] = max(task_sample["max_ms"], duration_ms)
                metadata = detail.get("metadata") or {}
                providers = metadata.get("providers") if isinstance(metadata, dict) else {}
                if not isinstance(providers, dict):
                    continue
                for provider_name, provider in providers.items():
                    if not isinstance(provider, dict):
                        continue
                    provider_status = str(provider.get("status") or "unknown")
                    latency_ms = max(0, int(provider.get("latency_ms") or 0))
                    _record_provider_sample(
                        provider_samples,
                        str(provider_name),
                        provider_status,
                        latency_ms,
                    )
                    if run_trigger == "schedule":
                        _record_provider_sample(
                            scheduled_provider_samples,
                            str(provider_name),
                            provider_status,
                            latency_ms,
                        )
        trigger_counts = Counter(_run_trigger(run.summary) for run in runs)
        scheduled_runs = [
            run for run in runs if _run_trigger(run.summary) == "schedule"
        ]
        schedule_intervals = _same_session_intervals(
            [run.started_at for run in scheduled_runs]
        )
        schedule_start_delays = [
            max(0, int((run.started_at - run.scheduled_at).total_seconds()))
            for run in scheduled_runs
        ]
        interval_p95 = _percentile_nearest_rank(schedule_intervals, 0.95)
        start_delay_p95 = _percentile_nearest_rank(schedule_start_delays, 0.95)
        if not scheduled_runs:
            scheduler_status = "unobserved"
        elif start_delay_p95 > 10 * 60 or interval_p95 > 20 * 60:
            scheduler_status = "degraded"
        elif not schedule_intervals:
            scheduler_status = "insufficient_history"
        else:
            scheduler_status = "healthy"
        runtime = {
            "runs_checked": len(runs),
            "successful_runs": sum(run.status == "success" for run in runs),
            "partial_runs": sum(run.status != "success" for run in runs),
            "trigger_counts": dict(sorted(trigger_counts.items())),
            "scheduler_status": scheduler_status,
            "schedule_runs_checked": len(scheduled_runs),
            "latest_schedule_started_at": (
                max(run.started_at for run in scheduled_runs).isoformat()
                if scheduled_runs
                else None
            ),
            "max_schedule_start_delay_seconds": max(
                schedule_start_delays,
                default=0,
            ),
            "average_schedule_start_delay_seconds": (
                int(sum(schedule_start_delays) / len(schedule_start_delays))
                if schedule_start_delays
                else 0
            ),
            "schedule_starts_over_10_minutes": sum(
                delay > 10 * 60 for delay in schedule_start_delays
            ),
            "p95_schedule_start_delay_seconds": start_delay_p95,
            "max_schedule_interval_seconds": max(schedule_intervals, default=0),
            "average_schedule_interval_seconds": (
                int(sum(schedule_intervals) / len(schedule_intervals))
                if schedule_intervals
                else 0
            ),
            "median_schedule_interval_seconds": (
                int(median(schedule_intervals)) if schedule_intervals else 0
            ),
            "p95_schedule_interval_seconds": interval_p95,
            "schedule_intervals_over_15_minutes": sum(
                interval > 15 * 60 for interval in schedule_intervals
            ),
            "planned_tasks": dict(sorted(planned.items())),
            "succeeded_tasks": dict(sorted(succeeded.items())),
            "task_execution": _finalize_latency_samples(task_samples),
            "provider_health": _finalize_latency_samples(provider_samples),
            "scheduled_provider_health": _finalize_latency_samples(
                scheduled_provider_samples
            ),
        }
        session_coverage = _regular_session_coverage(
            [run.started_at for run in scheduled_runs]
        )
        full_shadow_days = sum(
            bool(row["full_session_window_observed"])
            for row in session_coverage.values()
        )
        gates = {
            "message_contract": not violations,
            "messages_observed": bool(alerts),
            "scheduler_observed": bool(scheduled_runs),
            "scheduler_cadence_within_slo": scheduler_status == "healthy",
            "provider_health_observed": bool(scheduled_provider_samples),
            "provider_failures_zero": all(
                sample["failed"] == 0
                for sample in scheduled_provider_samples.values()
            ),
            "two_full_trading_days": full_shadow_days >= 2,
        }
        if all(gates.values()):
            validation_status = "ready_for_test_slack"
        elif gates["scheduler_observed"] and gates["provider_health_observed"]:
            validation_status = "observing"
        else:
            validation_status = "blocked"
        shadow_validation = {
            "stage": 5,
            "status": validation_status,
            "full_shadow_days": full_shadow_days,
            "required_full_shadow_days": 2,
            "gates": gates,
            "blocked_reasons": [name for name, passed in gates.items() if not passed],
            "regular_session_coverage": session_coverage,
        }
        return QualityReport(
            passed=not violations,
            messages_checked=len(alerts),
            message_violations=tuple(violations),
            recent_messages=tuple(samples),
            runtime=runtime,
            shadow_validation=shadow_validation,
            status_counts=self.repository.quality_status_counts(),
        )


def _finalize_latency_samples(
    samples: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    finalized = {}
    for name, sample in sorted(samples.items()):
        calls = sample["calls"]
        finalized[name] = {
            key: value
            for key, value in sample.items()
            if key != "total_ms"
        }
        finalized[name]["average_ms"] = int(sample["total_ms"] / calls) if calls else 0
    return finalized


def _record_provider_sample(
    samples: dict[str, dict[str, int]],
    name: str,
    status: str,
    latency_ms: int,
) -> None:
    sample = samples.setdefault(
        name,
        {
            "calls": 0,
            "success": 0,
            "degraded": 0,
            "recovered": 0,
            "failed": 0,
            "total_ms": 0,
            "max_ms": 0,
        },
    )
    sample["calls"] += 1
    if status in sample:
        sample[status] += 1
    sample["total_ms"] += latency_ms
    sample["max_ms"] = max(sample["max_ms"], latency_ms)


def _run_trigger(summary: object) -> str:
    if not isinstance(summary, dict):
        return "unknown"
    trigger = str(summary.get("trigger") or "unknown").strip().lower()
    return trigger or "unknown"


def _same_session_intervals(starts: list[datetime]) -> list[int]:
    eastern = ZoneInfo("America/New_York")
    ordered = sorted(starts)
    intervals = []
    for previous, current in zip(ordered, ordered[1:]):
        previous_local = previous.astimezone(eastern)
        current_local = current.astimezone(eastern)
        if previous_local.date() != current_local.date():
            continue
        intervals.append(max(0, int((current - previous).total_seconds())))
    return intervals


def _percentile_nearest_rank(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _regular_session_coverage(
    starts: list[datetime],
) -> dict[str, dict[str, object]]:
    eastern = ZoneInfo("America/New_York")
    sessions: dict[str, list[datetime]] = {}
    for started_at in starts:
        local = started_at.astimezone(eastern)
        if local.weekday() >= 5 or not time(9, 30) <= local.time() <= time(16, 15):
            continue
        sessions.setdefault(local.date().isoformat(), []).append(local)

    coverage = {}
    for trading_date, observations in sorted(sessions.items()):
        ordered = sorted(observations)
        first = ordered[0]
        last = ordered[-1]
        coverage[trading_date] = {
            "actual_polls": len(ordered),
            "nominal_regular_session_polls": 78,
            "coverage_percent": round(min(100.0, len(ordered) / 78 * 100), 1),
            "first_started_at": first.isoformat(),
            "last_started_at": last.isoformat(),
            "full_session_window_observed": (
                first.time() <= time(10, 0) and last.time() >= time(15, 30)
            ),
        }
    return coverage
