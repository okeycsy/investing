from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Protocol


@dataclass(frozen=True)
class TaskCheckpoint:
    checkpoint_key: str
    task_name: str
    last_success_at: datetime | None = None
    last_attempt_at: datetime | None = None
    last_error: str = ""
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class RunCheckpoint:
    run_id: str
    scheduled_at: datetime
    started_at: datetime
    completed_at: datetime | None
    status: str
    gap_seconds: int
    summary: Mapping[str, object] = field(default_factory=dict)
    build_sha: str = ""
    workflow_name: str = ""


class RuntimeRepository(Protocol):
    def task_checkpoints(self) -> dict[str, TaskCheckpoint]: ...

    def last_completed_run_at(self) -> datetime | None: ...

    def start_run(
        self,
        run_id: str,
        *,
        scheduled_at: datetime,
        started_at: datetime,
        gap_seconds: int,
    ) -> None: ...

    def finish_run(
        self,
        run_id: str,
        *,
        completed_at: datetime,
        status: str,
        summary: Mapping[str, object],
    ) -> None: ...

    def mark_task_started(
        self,
        checkpoint_key: str,
        task_name: str,
        attempted_at: datetime,
    ) -> None: ...

    def mark_task_succeeded(
        self,
        checkpoint_key: str,
        task_name: str,
        completed_at: datetime,
        metadata: Mapping[str, object] | None = None,
    ) -> None: ...

    def mark_task_failed(
        self,
        checkpoint_key: str,
        task_name: str,
        attempted_at: datetime,
        error: str,
    ) -> None: ...

    def recent_runs(self, limit: int = 10) -> list[RunCheckpoint]: ...
