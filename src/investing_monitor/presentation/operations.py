from __future__ import annotations

from collections.abc import Mapping


def build_tick_summary(payload: Mapping[str, object]) -> str:
    execution = _mapping(payload.get("execution"))
    plan = _mapping(execution.get("plan"))
    details = _mapping(execution.get("details"))
    delivery = _mapping(payload.get("delivery"))
    gap_seconds = int(plan.get("gap_seconds") or 0)
    failed = _mapping(execution.get("failed"))
    delivery_attention = bool(delivery.get("attention_required"))

    if failed:
        status = "DEGRADED"
    elif gap_seconds >= 15 * 60:
        status = "RECOVERED GAP"
    elif delivery_attention:
        status = "DELIVERY ATTENTION"
    else:
        status = "HEALTHY"

    rows = [
        ("운영 상태", status),
        ("실행 결과", str(execution.get("status") or "unknown")),
        ("schedule gap", _duration(gap_seconds)),
        ("schedule 시작 지연", _duration(int(payload.get("schedule_delay_seconds") or 0))),
    ]
    if delivery:
        rows.extend(
            (
                ("Slack 전송 성공", str(delivery.get("delivered") or 0)),
                ("Slack 재시도 예약", str(delivery.get("retry_scheduled") or 0)),
                ("Slack 결과 불명", str(delivery.get("outcome_unknown") or 0)),
                ("Slack 영구 거절", str(delivery.get("discarded") or 0)),
            )
        )

    lines = [
        "## Monitor v2 실행 요약",
        "",
        "| 항목 | 결과 |",
        "| --- | --- |",
        *(f"| {name} | {value} |" for name, value in rows),
        "",
        "### 작업",
        "",
        "| 작업 | 상태 | 시간 |",
        "| --- | --- | ---: |",
    ]
    if details:
        for detail in details.values():
            row = _mapping(detail)
            lines.append(
                f"| {row.get('task', 'unknown')} | {row.get('status', 'unknown')} | "
                f"{int(row.get('duration_ms') or 0):,} ms |"
            )
    else:
        lines.append("| 없음 | - | - |")
    return "\n".join(lines)


def build_quality_summary(payload: Mapping[str, object]) -> str:
    runtime = _mapping(payload.get("runtime"))
    current_build = _mapping(runtime.get("current_build_scheduler"))
    providers = _mapping(runtime.get("provider_state"))
    counts = _mapping(payload.get("status_counts"))
    outbox = _mapping(counts.get("outbox"))
    incidents = runtime.get("incidents") or []

    lines = [
        "## Monitor v2 운영 품질",
        "",
        f"**상태:** `{runtime.get('operational_status', 'unknown')}`",
        "",
        "| 지표 | 값 |",
        "| --- | ---: |",
        f"| 예약 run | {int(runtime.get('schedule_runs_checked') or 0)} |",
        f"| 현재 build 예약 run | {int(current_build.get('schedule_runs_checked') or 0)} |",
        f"| p95 run 간격 | {_duration(int(runtime.get('p95_schedule_interval_seconds') or 0))} |",
        "| p95 시작 지연 | "
        f"{_duration(int(runtime.get('p95_schedule_start_delay_seconds') or 0))} |",
        f"| 전송 대기 | {int(outbox.get('pending') or 0)} |",
        f"| 재시도 대기 | {int(outbox.get('failed') or 0)} |",
        f"| 결과 불명 | {int(outbox.get('delivery_unknown') or 0)} |",
        "",
        "### Provider",
        "",
        "| 이름 | 최근 상태 | 연속 실패 | 마지막 성공 |",
        "| --- | --- | ---: | --- |",
    ]
    if providers:
        for name, raw_state in providers.items():
            state = _mapping(raw_state)
            lines.append(
                f"| {name} | {state.get('latest_status', 'unknown')} | "
                f"{int(state.get('consecutive_failures') or 0)} | "
                f"{state.get('last_success_at') or '-'} |"
            )
    else:
        lines.append("| 관측 전 | - | 0 | - |")

    if incidents:
        lines.extend(("", "### Incident", ""))
        for raw_incident in incidents:
            incident = _mapping(raw_incident)
            if incident.get("type") == "schedule_gap":
                lines.append(
                    f"- schedule gap {_duration(int(incident.get('gap_seconds') or 0))} "
                    f"(run `{incident.get('run_id', '')}`)"
                )
            elif incident.get("type") == "provider_failure":
                lines.append(
                    f"- `{incident.get('provider', 'unknown')}` "
                    f"{int(incident.get('consecutive_failures') or 0)}회 연속 실패"
                )
    return "\n".join(lines)


def quality_annotations(payload: Mapping[str, object]) -> tuple[str, ...]:
    runtime = _mapping(payload.get("runtime"))
    annotations = []
    for raw_incident in runtime.get("incidents") or []:
        incident = _mapping(raw_incident)
        if incident.get("type") == "schedule_gap":
            annotations.append(
                "::warning title=Monitor v2 schedule gap::"
                f"Next tick recovered after {_duration(int(incident.get('gap_seconds') or 0))}"
            )
        elif incident.get("type") == "provider_failure":
            annotations.append(
                "::warning title=Monitor v2 provider incident::"
                f"{incident.get('provider', 'unknown')} has "
                f"{int(incident.get('consecutive_failures') or 0)} consecutive failures"
            )
    return tuple(annotations)


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _duration(seconds: int) -> str:
    seconds = max(0, seconds)
    if seconds < 60:
        return f"{seconds}초"
    minutes, remaining_seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}분 {remaining_seconds}초" if remaining_seconds else f"{minutes}분"
    hours, remaining_minutes = divmod(minutes, 60)
    return (
        f"{hours}시간 {remaining_minutes}분"
        if remaining_minutes
        else f"{hours}시간"
    )
