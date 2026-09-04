from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from math import ceil
from statistics import median
from typing import Protocol
from zoneinfo import ZoneInfo

from investing_monitor.ports.repository import (
    AlertRecord,
    EvidenceQualityRecord,
    MarketObservationRecord,
    MonitorRepository,
)
from investing_monitor.presentation.quality import audit_message


class MarketSessionCalendar(Protocol):
    def regular_open(self, value: date) -> datetime: ...

    def regular_close(self, value: date) -> datetime: ...


class StandardMarketSessionCalendar:
    def regular_open(self, value: date) -> datetime:
        return datetime.combine(value, time(9, 30), ZoneInfo("America/New_York"))

    def regular_close(self, value: date) -> datetime:
        return datetime.combine(value, time(16, 0), ZoneInfo("America/New_York"))


@dataclass(frozen=True)
class QualityReport:
    passed: bool
    messages_checked: int
    message_violations: tuple[dict[str, object], ...]
    recent_messages: tuple[dict[str, object], ...]
    runtime: dict[str, object]
    product_quality: dict[str, object]
    shadow_validation: dict[str, object]
    status_counts: dict[str, dict[str, int]]

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "messages_checked": self.messages_checked,
            "message_violations": list(self.message_violations),
            "recent_messages": list(self.recent_messages),
            "runtime": self.runtime,
            "product_quality": self.product_quality,
            "shadow_validation": self.shadow_validation,
            "status_counts": self.status_counts,
        }


class QualityReportService:
    def __init__(
        self,
        repository: MonitorRepository,
        *,
        calendar: MarketSessionCalendar | None = None,
    ) -> None:
        self.repository = repository
        self.calendar = calendar or StandardMarketSessionCalendar()

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
        market_recovery = {
            "task_runs": 0,
            "scheduled_task_runs": 0,
            "runs_with_replay": 0,
            "scheduled_runs_with_replay": 0,
            "observed_frames": 0,
            "replayed_frames": 0,
            "max_replayed_frames_in_run": 0,
            "max_recovery_span_minutes": 0,
            "max_source_age_seconds": 0,
            "max_gap_recovered_seconds": 0,
        }
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
                if not isinstance(metadata, dict):
                    metadata = {}
                if task_name == "market" and task_status == "success":
                    observed_frames = max(0, int(metadata.get("observed_frames") or 0))
                    replayed_frames = max(0, int(metadata.get("replayed_frames") or 0))
                    market_recovery["task_runs"] += 1
                    market_recovery["scheduled_task_runs"] += int(
                        run_trigger == "schedule"
                    )
                    market_recovery["runs_with_replay"] += int(replayed_frames > 0)
                    market_recovery["scheduled_runs_with_replay"] += int(
                        run_trigger == "schedule" and replayed_frames > 0
                    )
                    market_recovery["observed_frames"] += observed_frames
                    market_recovery["replayed_frames"] += replayed_frames
                    market_recovery["max_replayed_frames_in_run"] = max(
                        market_recovery["max_replayed_frames_in_run"],
                        replayed_frames,
                    )
                    market_recovery["max_recovery_span_minutes"] = max(
                        market_recovery["max_recovery_span_minutes"],
                        replayed_frames * 5,
                    )
                    market_recovery["max_source_age_seconds"] = max(
                        market_recovery["max_source_age_seconds"],
                        max(0, int(metadata.get("source_age_seconds") or 0)),
                    )
                    if replayed_frames:
                        market_recovery["max_gap_recovered_seconds"] = max(
                            market_recovery["max_gap_recovered_seconds"],
                            run.gap_seconds,
                        )
                providers = metadata.get("providers")
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
            "market_recovery": market_recovery,
        }
        poll_coverage = _regular_session_coverage(
            [run.started_at for run in scheduled_runs]
        )
        data_coverage = _regular_market_data_coverage(
            self.repository.recent_market_observations(limit_days=10),
            self.calendar,
        )
        full_shadow_days = sum(
            bool(row["full_session_data_recovered"])
            for row in data_coverage.values()
        )
        product_quality = _product_quality(
            alerts,
            self.repository.recent_evidence_quality_records(limit=max(500, limit * 5)),
            data_coverage,
        )
        recent_semantic_violations = int(
            product_quality["evidence_alerts"]["recent_semantic_violations"]
        )
        days_over_alert_target = int(
            product_quality["alert_load"]["days_over_target"]
        )
        gates = {
            "message_contract": not violations,
            "messages_observed": bool(alerts),
            "scheduler_observed": bool(scheduled_runs),
            "provider_health_observed": bool(scheduled_provider_samples),
            "provider_failures_zero": all(
                sample["failed"] == 0
                for sample in scheduled_provider_samples.values()
            ),
            "two_full_trading_days": full_shadow_days >= 2,
            "recent_evidence_alerts_clean": recent_semantic_violations == 0,
            "alert_load_within_target": days_over_alert_target == 0,
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
            "advisories": {
                "scheduler_cadence_within_slo": scheduler_status == "healthy",
            },
            "regular_session_poll_coverage": poll_coverage,
            "regular_session_data_coverage": data_coverage,
        }
        return QualityReport(
            passed=not violations,
            messages_checked=len(alerts),
            message_violations=tuple(violations),
            recent_messages=tuple(samples),
            runtime=runtime,
            product_quality=product_quality,
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


def _regular_market_data_coverage(
    observations: list[MarketObservationRecord],
    calendar: MarketSessionCalendar,
) -> dict[str, dict[str, object]]:
    if not observations:
        return {}
    latest_ticker = max(observations, key=lambda item: item.observed_at).ticker
    regular = [
        item
        for item in observations
        if item.ticker == latest_ticker and item.session == "regular"
    ]
    sessions: dict[str, list[MarketObservationRecord]] = {}
    for item in regular:
        sessions.setdefault(item.trading_date.isoformat(), []).append(item)

    coverage = {}
    for trading_date, rows in sorted(sessions.items()):
        trading_day = date.fromisoformat(trading_date)
        regular_open = calendar.regular_open(trading_day)
        regular_close = calendar.regular_close(trading_day)
        expected_buckets = int(
            (regular_close - regular_open).total_seconds() // (5 * 60)
        )
        buckets: set[int] = set()
        for row in rows:
            observed_at = row.observed_at.astimezone(regular_open.tzinfo)
            second_offset = int((observed_at - regular_open).total_seconds())
            if 0 <= second_offset < int(
                (regular_close - regular_open).total_seconds()
            ):
                buckets.add(second_offset // (5 * 60))
        ordered = sorted(rows, key=lambda item: item.observed_at)
        covered = len(buckets)
        coverage[trading_date] = {
            "ticker": latest_ticker,
            "observed_rows": len(rows),
            "covered_5m_buckets": covered,
            "nominal_regular_5m_buckets": expected_buckets,
            "coverage_percent": round(min(100.0, covered / expected_buckets * 100), 1),
            "first_bar_at": ordered[0].observed_at.isoformat(),
            "last_bar_at": ordered[-1].observed_at.isoformat(),
            "full_session_data_recovered": (
                covered == expected_buckets
                and 0 in buckets
                and expected_buckets - 1 in buckets
            ),
        }
    return coverage


def _product_quality(
    alerts: list[AlertRecord],
    evidence_records: list[EvidenceQualityRecord],
    data_coverage: dict[str, dict[str, object]],
) -> dict[str, object]:
    full_dates = sorted(
        trading_date
        for trading_date, row in data_coverage.items()
        if row["full_session_data_recovered"]
    )
    evaluation_dates = set(full_dates[-2:])
    evidence_by_url: dict[str, EvidenceQualityRecord] = {}
    for record in evidence_records:
        if record.source_url:
            evidence_by_url.setdefault(record.source_url, record)

    classifications = Counter()
    findings = []
    evidence_alert_types = {"catalyst", "filing", "insider"}
    for alert in alerts:
        if alert.alert_type not in evidence_alert_types:
            continue
        source_url = _message_source_url(alert.payload)
        record = evidence_by_url.get(source_url)
        classification = "unassessed"
        reason = "source evidence is unavailable in the retained quality window"
        current_cluster = ""
        if record is not None:
            current_cluster = record.cluster_key
            if record.status == "filtered":
                classification = "retrospectively_filtered"
                reason = record.status_reason or "current evidence rule filters this item"
            elif record.status == "analyzed" and record.relevant is True:
                if record.cluster_key == alert.event_key:
                    classification = "currently_valid"
                    reason = "current evidence rule retains this canonical event"
                else:
                    classification = "duplicate_cluster_reconciled"
                    reason = "source was relinked to an earlier canonical event"
            else:
                classification = "no_longer_relevant"
                reason = record.status_reason or "current evidence state is not alertable"
        classifications[classification] += 1
        local_date = alert.created_at.astimezone(
            ZoneInfo("America/New_York")
        ).date().isoformat()
        findings.append(
            {
                "event_key": alert.event_key,
                "created_at": alert.created_at.isoformat(),
                "trading_date": local_date,
                "classification": classification,
                "reason": reason,
                "source_url": source_url,
                "current_cluster": current_cluster,
            }
        )

    assessed = sum(
        count
        for classification, count in classifications.items()
        if classification != "unassessed"
    )
    valid = classifications["currently_valid"]
    duplicates = classifications["duplicate_cluster_reconciled"]
    semantic_violations = {
        "retrospectively_filtered",
        "duplicate_cluster_reconciled",
        "no_longer_relevant",
    }
    recent_findings = [
        finding
        for finding in findings
        if finding["trading_date"] in evaluation_dates
    ]
    recent_semantic_violations = sum(
        finding["classification"] in semantic_violations
        for finding in recent_findings
    )

    routine_types = {"daily_close", "weekly_review"}
    load_by_day = {}
    for trading_date in full_dates[-5:]:
        day_alerts = [
            alert
            for alert in alerts
            if alert.created_at.astimezone(ZoneInfo("America/New_York"))
            .date()
            .isoformat()
            == trading_date
        ]
        by_type = Counter(alert.alert_type for alert in day_alerts)
        nonroutine = sum(
            count for alert_type, count in by_type.items() if alert_type not in routine_types
        )
        load_by_day[trading_date] = {
            "total_alerts": len(day_alerts),
            "routine_alerts": len(day_alerts) - nonroutine,
            "nonroutine_alerts": nonroutine,
            "target_max_nonroutine_alerts": 3,
            "within_target": nonroutine <= 3,
            "by_type": dict(sorted(by_type.items())),
        }
    days_over_target = sum(
        not row["within_target"] for row in load_by_day.values()
    )

    if recent_semantic_violations or days_over_target:
        status = "needs_improvement"
    elif len(evaluation_dates) >= 2:
        status = "meets_current_targets"
    else:
        status = "observing"
    return {
        "status": status,
        "evaluation_trading_dates": sorted(evaluation_dates),
        "evidence_alerts": {
            "alerts_checked": len(findings),
            "assessed": assessed,
            "currently_valid": valid,
            "retrospectively_filtered": classifications[
                "retrospectively_filtered"
            ],
            "duplicate_cluster_reconciled": duplicates,
            "no_longer_relevant": classifications["no_longer_relevant"],
            "unassessed": classifications["unassessed"],
            "meaningful_rate_percent": (
                round(valid / assessed * 100, 1) if assessed else None
            ),
            "target_meaningful_rate_percent": 80.0,
            "duplicate_rate_percent": (
                round(duplicates / assessed * 100, 1) if assessed else None
            ),
            "target_duplicate_rate_percent": 0.1,
            "recent_semantic_violations": recent_semantic_violations,
            "findings": findings,
        },
        "alert_load": {
            "completed_trading_days_checked": len(load_by_day),
            "days_over_target": days_over_target,
            "target_nonroutine_alerts_per_day": "0-3",
            "by_trading_day": load_by_day,
        },
    }


def _message_source_url(payload: object) -> str:
    if isinstance(payload, str):
        match = re.search(r"<(https?://[^|>]+)(?:\|[^>]*)?>", payload)
        return match.group(1) if match else ""
    if isinstance(payload, dict):
        for value in payload.values():
            found = _message_source_url(value)
            if found:
                return found
    if isinstance(payload, list):
        for value in payload:
            found = _message_source_url(value)
            if found:
                return found
    return ""
