from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
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
    status_counts: dict[str, dict[str, int]]

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "messages_checked": self.messages_checked,
            "message_violations": list(self.message_violations),
            "recent_messages": list(self.recent_messages),
            "runtime": self.runtime,
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
        for run in runs:
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
                    sample = provider_samples.setdefault(
                        str(provider_name),
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
                    if provider_status in sample:
                        sample[provider_status] += 1
                    sample["total_ms"] += latency_ms
                    sample["max_ms"] = max(sample["max_ms"], latency_ms)
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
        if not scheduled_runs:
            scheduler_status = "unobserved"
        elif max(schedule_start_delays, default=0) > 10 * 60 or max(
            schedule_intervals, default=0
        ) > 15 * 60:
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
            "max_schedule_interval_seconds": max(schedule_intervals, default=0),
            "average_schedule_interval_seconds": (
                int(sum(schedule_intervals) / len(schedule_intervals))
                if schedule_intervals
                else 0
            ),
            "schedule_intervals_over_15_minutes": sum(
                interval > 15 * 60 for interval in schedule_intervals
            ),
            "planned_tasks": dict(sorted(planned.items())),
            "succeeded_tasks": dict(sorted(succeeded.items())),
            "task_execution": _finalize_latency_samples(task_samples),
            "provider_health": _finalize_latency_samples(provider_samples),
        }
        return QualityReport(
            passed=not violations,
            messages_checked=len(alerts),
            message_violations=tuple(violations),
            recent_messages=tuple(samples),
            runtime=runtime,
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
