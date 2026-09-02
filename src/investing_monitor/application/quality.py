from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

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
        delays = [
            max(0, int((run.started_at - run.scheduled_at).total_seconds()))
            for run in runs
        ]
        runtime = {
            "runs_checked": len(runs),
            "successful_runs": sum(run.status == "success" for run in runs),
            "partial_runs": sum(run.status != "success" for run in runs),
            "max_schedule_delay_seconds": max(delays, default=0),
            "average_schedule_delay_seconds": (
                int(sum(delays) / len(delays)) if delays else 0
            ),
            "gap_runs_over_10_minutes": sum(run.gap_seconds >= 600 for run in runs),
            "planned_tasks": dict(sorted(planned.items())),
            "succeeded_tasks": dict(sorted(succeeded.items())),
        }
        return QualityReport(
            passed=not violations,
            messages_checked=len(alerts),
            message_violations=tuple(violations),
            recent_messages=tuple(samples),
            runtime=runtime,
            status_counts=self.repository.quality_status_counts(),
        )
