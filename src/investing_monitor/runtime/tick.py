from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from time import monotonic as monotonic_clock
from typing import Protocol
from zoneinfo import ZoneInfo

from investing_monitor.ports.runtime import RuntimeRepository, TaskCheckpoint


NEW_YORK = ZoneInfo("America/New_York")
SEOUL = ZoneInfo("Asia/Seoul")


class TickTask(str, Enum):
    RECOVERY = "recovery"
    MARKET = "market"
    NEWS = "news"
    SEC = "sec"
    CLOSE = "close"
    WEEKLY = "weekly"
    THIRTEEN_F = "13f"
    DELIVERY = "delivery"


@dataclass(frozen=True)
class PlannedTask:
    name: TickTask
    checkpoint_key: str
    due_since: datetime | None = None


@dataclass(frozen=True)
class TickPlan:
    now: datetime
    gap_seconds: int
    tasks: tuple[PlannedTask, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "now": self.now.astimezone(timezone.utc).isoformat(),
            "gap_seconds": self.gap_seconds,
            "tasks": [
                {
                    "name": task.name.value,
                    "checkpoint_key": task.checkpoint_key,
                    "due_since": (
                        task.due_since.astimezone(timezone.utc).isoformat()
                        if task.due_since
                        else None
                    ),
                }
                for task in self.tasks
            ],
        }


@dataclass(frozen=True)
class TickSchedule:
    market_interval: timedelta = timedelta(minutes=5)
    active_news_interval: timedelta = timedelta(minutes=5)
    active_sec_interval: timedelta = timedelta(minutes=10)
    off_hours_source_interval: timedelta = timedelta(minutes=30)
    recovery_gap: timedelta = timedelta(minutes=10)
    close_delay: timedelta = timedelta(minutes=15)
    stale_close_after: timedelta = timedelta(hours=16)


class TradingCalendar(Protocol):
    def is_trading_day(self, value: date) -> bool: ...

    def regular_close(self, value: date) -> datetime: ...


class WeekdayTradingCalendar:
    """Fallback calendar used until the exchange-calendar adapter is connected."""

    def is_trading_day(self, value: date) -> bool:
        return value.weekday() < 5

    def regular_close(self, value: date) -> datetime:
        return datetime.combine(value, time(16, 0), tzinfo=NEW_YORK)


class TickPlanner:
    def __init__(
        self,
        *,
        schedule: TickSchedule | None = None,
        calendar: TradingCalendar | None = None,
        enabled_tasks: set[TickTask] | frozenset[TickTask] | None = None,
    ) -> None:
        self.schedule = schedule or TickSchedule()
        self.calendar = calendar or WeekdayTradingCalendar()
        self.enabled_tasks = (
            frozenset(TickTask) if enabled_tasks is None else frozenset(enabled_tasks)
        )

    def plan(
        self,
        now: datetime,
        checkpoints: Mapping[str, TaskCheckpoint],
        *,
        last_completed_run_at: datetime | None,
    ) -> TickPlan:
        now = _aware_utc(now)
        gap_seconds = _gap_seconds(now, last_completed_run_at)
        tasks: list[PlannedTask] = []

        if (
            TickTask.RECOVERY in self.enabled_tasks
            and last_completed_run_at
            and gap_seconds >= int(self.schedule.recovery_gap.total_seconds())
        ):
            tasks.append(
                PlannedTask(
                    TickTask.RECOVERY,
                    f"recovery:{int(last_completed_run_at.timestamp())}",
                    last_completed_run_at,
                )
            )

        ny_now = now.astimezone(NEW_YORK)
        trading_day = self.calendar.is_trading_day(ny_now.date())
        active_session = trading_day and time(4, 0) <= ny_now.time() < time(20, 0)

        if TickTask.MARKET in self.enabled_tasks and active_session and _is_due(
            checkpoints.get(TickTask.MARKET.value),
            now,
            self.schedule.market_interval,
        ):
            tasks.append(PlannedTask(TickTask.MARKET, TickTask.MARKET.value))

        news_interval = (
            self.schedule.active_news_interval
            if active_session
            else self.schedule.off_hours_source_interval
        )
        if TickTask.NEWS in self.enabled_tasks and _is_due(
            checkpoints.get(TickTask.NEWS.value), now, news_interval
        ):
            tasks.append(PlannedTask(TickTask.NEWS, TickTask.NEWS.value))

        sec_interval = (
            self.schedule.active_sec_interval
            if active_session
            else self.schedule.off_hours_source_interval
        )
        if TickTask.SEC in self.enabled_tasks and _is_due(
            checkpoints.get(TickTask.SEC.value), now, sec_interval
        ):
            tasks.append(PlannedTask(TickTask.SEC, TickTask.SEC.value))

        close_date = self._due_close_date(ny_now)
        if TickTask.CLOSE in self.enabled_tasks and close_date is not None:
            close_key = f"close:{close_date.isoformat()}"
            if not _was_successful(checkpoints.get(close_key)):
                close_at = self.calendar.regular_close(close_date) + self.schedule.close_delay
                tasks.append(PlannedTask(TickTask.CLOSE, close_key, close_at))

        seoul_now = now.astimezone(SEOUL)
        weekly_key = f"weekly:{seoul_now.strftime('%G-W%V')}"
        if (
            TickTask.WEEKLY in self.enabled_tasks
            and seoul_now.weekday() == 0
            and seoul_now.time() >= time(8, 10)
            and not _was_successful(checkpoints.get(weekly_key))
        ):
            tasks.append(PlannedTask(TickTask.WEEKLY, weekly_key))

        thirteen_f_key = f"13f:{seoul_now.strftime('%G-W%V')}"
        if (
            TickTask.THIRTEEN_F in self.enabled_tasks
            and seoul_now.weekday() == 5
            and seoul_now.time() >= time(19, 0)
            and not _was_successful(checkpoints.get(thirteen_f_key))
        ):
            tasks.append(PlannedTask(TickTask.THIRTEEN_F, thirteen_f_key))

        if TickTask.DELIVERY in self.enabled_tasks:
            tasks.append(PlannedTask(TickTask.DELIVERY, TickTask.DELIVERY.value))
        return TickPlan(now=now, gap_seconds=gap_seconds, tasks=tuple(tasks))

    def _due_close_date(self, ny_now: datetime) -> date | None:
        today = ny_now.date()
        today_is_trading = self.calendar.is_trading_day(today)
        today_close_at = (
            self.calendar.regular_close(today) + self.schedule.close_delay
            if today_is_trading
            else None
        )
        candidate = (
            today
            if today_close_at is not None and ny_now >= today_close_at
            else self._previous_trading_day(today)
        )
        close_at = self.calendar.regular_close(candidate) + self.schedule.close_delay
        if ny_now < close_at or ny_now - close_at > self.schedule.stale_close_after:
            return None
        return candidate

    def _previous_trading_day(self, value: date) -> date:
        candidate = value - timedelta(days=1)
        while not self.calendar.is_trading_day(candidate):
            candidate -= timedelta(days=1)
        return candidate


TaskHandler = Callable[[PlannedTask], Mapping[str, object] | None]


class TaskExecutionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.metadata = dict(metadata or {})


@dataclass(frozen=True)
class TickExecutionReport:
    run_id: str
    status: str
    plan: TickPlan
    trigger: str = "manual"
    succeeded: tuple[str, ...] = ()
    failed: Mapping[str, str] = field(default_factory=dict)
    skipped: tuple[str, ...] = ()
    details: Mapping[str, Mapping[str, object]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "trigger": self.trigger,
            "plan": self.plan.as_dict(),
            "succeeded": list(self.succeeded),
            "failed": dict(self.failed),
            "skipped": list(self.skipped),
            "details": {key: dict(value) for key, value in self.details.items()},
        }


class TickRunner:
    def __init__(
        self,
        repository: RuntimeRepository,
        handlers: Mapping[TickTask, TaskHandler],
        *,
        planner: TickPlanner | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.repository = repository
        self.handlers = handlers
        self.planner = planner or TickPlanner()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic or monotonic_clock

    def run(
        self,
        run_id: str,
        *,
        scheduled_at: datetime,
        started_at: datetime | None = None,
        trigger: str = "manual",
    ) -> TickExecutionReport:
        started_at = _aware_utc(started_at or self.clock())
        last_run = self.repository.last_completed_run_at()
        plan = self.planner.plan(
            started_at,
            self.repository.task_checkpoints(),
            last_completed_run_at=last_run,
        )
        self.repository.start_run(
            run_id,
            scheduled_at=_aware_utc(scheduled_at),
            started_at=started_at,
            gap_seconds=plan.gap_seconds,
        )

        succeeded: list[str] = []
        failed: dict[str, str] = {}
        skipped: list[str] = []
        details: dict[str, Mapping[str, object]] = {}
        for task in plan.tasks:
            handler = self.handlers.get(task.name)
            if handler is None:
                skipped.append(task.checkpoint_key)
                details[task.checkpoint_key] = {
                    "task": task.name.value,
                    "status": "skipped",
                    "duration_ms": 0,
                }
                continue
            attempted_at = _aware_utc(self.clock())
            self.repository.mark_task_started(task.checkpoint_key, task.name.value, attempted_at)
            started_monotonic = self.monotonic()
            try:
                metadata = handler(task) or {}
            except Exception as exc:  # task isolation is the runtime contract
                failure_metadata = (
                    exc.metadata if isinstance(exc, TaskExecutionError) else {}
                )
                duration_ms = max(
                    0,
                    int((self.monotonic() - started_monotonic) * 1_000),
                )
                failed[task.checkpoint_key] = str(exc)
                details[task.checkpoint_key] = {
                    "task": task.name.value,
                    "status": "failed",
                    "duration_ms": duration_ms,
                    "error": str(exc),
                }
                if failure_metadata:
                    details[task.checkpoint_key]["metadata"] = failure_metadata
                self.repository.mark_task_failed(
                    task.checkpoint_key,
                    task.name.value,
                    attempted_at,
                    str(exc),
                    failure_metadata,
                )
                continue
            completed_at = _aware_utc(self.clock())
            duration_ms = max(
                0,
                int((self.monotonic() - started_monotonic) * 1_000),
            )
            self.repository.mark_task_succeeded(
                task.checkpoint_key,
                task.name.value,
                completed_at,
                metadata,
            )
            succeeded.append(task.checkpoint_key)
            details[task.checkpoint_key] = {
                "task": task.name.value,
                "status": "success",
                "duration_ms": duration_ms,
                "metadata": dict(metadata),
            }

        status = "partial" if failed or skipped else "success"
        report = TickExecutionReport(
            run_id=run_id,
            status=status,
            plan=plan,
            trigger=trigger.strip().lower() or "manual",
            succeeded=tuple(succeeded),
            failed=failed,
            skipped=tuple(skipped),
            details=details,
        )
        self.repository.finish_run(
            run_id,
            completed_at=_aware_utc(self.clock()),
            status=status,
            summary=report.as_dict(),
        )
        return report


def _is_due(checkpoint: TaskCheckpoint | None, now: datetime, interval: timedelta) -> bool:
    if checkpoint is None or checkpoint.last_success_at is None:
        return True
    return now - checkpoint.last_success_at.astimezone(timezone.utc) >= interval


def _was_successful(checkpoint: TaskCheckpoint | None) -> bool:
    return checkpoint is not None and checkpoint.last_success_at is not None


def _gap_seconds(now: datetime, last_completed_run_at: datetime | None) -> int:
    if last_completed_run_at is None:
        return 0
    return max(
        0,
        int((now - last_completed_run_at.astimezone(timezone.utc)).total_seconds()),
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("tick timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
