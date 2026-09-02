from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping

from investing_monitor.domain.models import PriceBandSignal, PriceBandState
from investing_monitor.ports.repository import PendingDelivery
from investing_monitor.ports.runtime import RunCheckpoint, TaskCheckpoint


SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_sessions (
    ticker TEXT PRIMARY KEY,
    trading_date TEXT NOT NULL,
    upward_high_watermark INTEGER NOT NULL DEFAULT 0,
    downward_high_watermark INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    event_key TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    delivered_at TEXT,
    receipt TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    delivery_status TEXT NOT NULL DEFAULT 'pending',
    attempted_at TEXT,
    FOREIGN KEY(event_key) REFERENCES alerts(event_key)
);

CREATE TABLE IF NOT EXISTS task_checkpoints (
    checkpoint_key TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    last_success_at TEXT,
    last_attempt_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS run_checkpoints (
    run_id TEXT PRIMARY KEY,
    scheduled_at TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    gap_seconds INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_run_checkpoints_completed
ON run_checkpoints(completed_at DESC);
"""


class SQLiteMonitorRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection, connection:
            connection.executescript(SCHEMA)
            self._ensure_column(
                connection,
                "outbox",
                "delivery_status",
                "TEXT NOT NULL DEFAULT 'pending'",
            )
            self._ensure_column(connection, "outbox", "attempted_at", "TEXT")
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def load_price_band_state(self, ticker: str) -> PriceBandState | None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT trading_date, upward_high_watermark, downward_high_watermark "
                "FROM market_sessions WHERE ticker = ?",
                (ticker.upper(),),
            ).fetchone()
        if row is None:
            return None
        return PriceBandState(
            trading_date=date.fromisoformat(row["trading_date"]),
            upward_high_watermark=row["upward_high_watermark"],
            downward_high_watermark=row["downward_high_watermark"],
        )

    def record_price_signal(
        self,
        signal: PriceBandSignal,
        state: PriceBandState,
        payload: dict,
    ) -> bool:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            inserted = connection.execute(
                "INSERT OR IGNORE INTO alerts "
                "(event_key, ticker, alert_type, created_at, payload_json) "
                "VALUES (?, ?, 'price_band', ?, ?)",
                (signal.event_key, signal.ticker, now, payload_json),
            ).rowcount
            if not inserted:
                return False
            connection.execute(
                "INSERT INTO market_sessions "
                "(ticker, trading_date, upward_high_watermark, downward_high_watermark, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(ticker) DO UPDATE SET "
                "trading_date = excluded.trading_date, "
                "upward_high_watermark = excluded.upward_high_watermark, "
                "downward_high_watermark = excluded.downward_high_watermark, "
                "updated_at = excluded.updated_at",
                (
                    signal.ticker,
                    state.trading_date.isoformat(),
                    state.upward_high_watermark,
                    state.downward_high_watermark,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO outbox (event_key, payload_json, next_attempt_at) VALUES (?, ?, ?)",
                (signal.event_key, payload_json, now),
            )
        return True

    def pending_deliveries(self, now: datetime, limit: int = 20) -> list[PendingDelivery]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT id, event_key, payload_json, attempts, delivery_status FROM outbox "
                "WHERE delivered_at IS NULL "
                "AND delivery_status IN ('pending', 'failed') "
                "AND next_attempt_at <= ? "
                "ORDER BY id LIMIT ?",
                (now.astimezone(timezone.utc).isoformat(), limit),
            ).fetchall()
        return [
            PendingDelivery(
                outbox_id=row["id"],
                event_key=row["event_key"],
                payload=json.loads(row["payload_json"]),
                attempts=row["attempts"],
                status=row["delivery_status"],
            )
            for row in rows
        ]

    def mark_sending(self, outbox_id: int, attempted_at: datetime) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE outbox SET delivery_status = 'sending', attempted_at = ?, "
                "attempts = attempts + 1, last_error = '' WHERE id = ?",
                (attempted_at.astimezone(timezone.utc).isoformat(), outbox_id),
            )

    def mark_delivered(self, outbox_id: int, delivered_at: datetime, receipt: str = "") -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE outbox SET delivered_at = ?, receipt = ?, last_error = '', "
                "delivery_status = 'delivered' WHERE id = ?",
                (delivered_at.astimezone(timezone.utc).isoformat(), receipt, outbox_id),
            )

    def mark_failed(self, outbox_id: int, next_attempt_at: datetime, error: str) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE outbox SET attempts = attempts + "
                "CASE WHEN delivery_status = 'sending' THEN 0 ELSE 1 END, "
                "next_attempt_at = ?, last_error = ?, "
                "delivery_status = 'failed' "
                "WHERE id = ?",
                (next_attempt_at.astimezone(timezone.utc).isoformat(), error[:1000], outbox_id),
            )

    def mark_delivery_unknown(
        self,
        outbox_id: int,
        attempted_at: datetime,
        error: str,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE outbox SET delivery_status = 'delivery_unknown', attempted_at = ?, "
                "last_error = ? WHERE id = ?",
                (
                    attempted_at.astimezone(timezone.utc).isoformat(),
                    error[:1000],
                    outbox_id,
                ),
            )

    def task_checkpoints(self) -> dict[str, TaskCheckpoint]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT checkpoint_key, task_name, last_success_at, last_attempt_at, "
                "last_error, metadata_json FROM task_checkpoints"
            ).fetchall()
        return {
            row["checkpoint_key"]: TaskCheckpoint(
                checkpoint_key=row["checkpoint_key"],
                task_name=row["task_name"],
                last_success_at=_parse_datetime(row["last_success_at"]),
                last_attempt_at=_parse_datetime(row["last_attempt_at"]),
                last_error=row["last_error"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        }

    def last_completed_run_at(self) -> datetime | None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT completed_at FROM run_checkpoints "
                "WHERE completed_at IS NOT NULL ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
        return _parse_datetime(row["completed_at"]) if row else None

    def start_run(
        self,
        run_id: str,
        *,
        scheduled_at: datetime,
        started_at: datetime,
        gap_seconds: int,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "INSERT INTO run_checkpoints "
                "(run_id, scheduled_at, started_at, status, gap_seconds) "
                "VALUES (?, ?, ?, 'running', ?) "
                "ON CONFLICT(run_id) DO UPDATE SET "
                "scheduled_at = excluded.scheduled_at, started_at = excluded.started_at, "
                "completed_at = NULL, status = 'running', gap_seconds = excluded.gap_seconds, "
                "summary_json = '{}'",
                (
                    run_id,
                    _utc_iso(scheduled_at),
                    _utc_iso(started_at),
                    max(0, gap_seconds),
                ),
            )

    def finish_run(
        self,
        run_id: str,
        *,
        completed_at: datetime,
        status: str,
        summary: Mapping[str, object],
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE run_checkpoints SET completed_at = ?, status = ?, summary_json = ? "
                "WHERE run_id = ?",
                (
                    _utc_iso(completed_at),
                    status,
                    json.dumps(summary, ensure_ascii=False, separators=(",", ":")),
                    run_id,
                ),
            )

    def mark_task_started(
        self,
        checkpoint_key: str,
        task_name: str,
        attempted_at: datetime,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "INSERT INTO task_checkpoints "
                "(checkpoint_key, task_name, last_attempt_at) VALUES (?, ?, ?) "
                "ON CONFLICT(checkpoint_key) DO UPDATE SET "
                "task_name = excluded.task_name, "
                "last_attempt_at = excluded.last_attempt_at, last_error = ''",
                (checkpoint_key, task_name, _utc_iso(attempted_at)),
            )

    def mark_task_succeeded(
        self,
        checkpoint_key: str,
        task_name: str,
        completed_at: datetime,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        serialized = json.dumps(metadata or {}, ensure_ascii=False, separators=(",", ":"))
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "INSERT INTO task_checkpoints "
                "(checkpoint_key, task_name, last_success_at, last_attempt_at, metadata_json) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(checkpoint_key) DO UPDATE SET "
                "task_name = excluded.task_name, last_success_at = excluded.last_success_at, "
                "last_attempt_at = excluded.last_attempt_at, last_error = '', "
                "metadata_json = excluded.metadata_json",
                (
                    checkpoint_key,
                    task_name,
                    _utc_iso(completed_at),
                    _utc_iso(completed_at),
                    serialized,
                ),
            )

    def mark_task_failed(
        self,
        checkpoint_key: str,
        task_name: str,
        attempted_at: datetime,
        error: str,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "INSERT INTO task_checkpoints "
                "(checkpoint_key, task_name, last_attempt_at, last_error) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(checkpoint_key) DO UPDATE SET "
                "task_name = excluded.task_name, "
                "last_attempt_at = excluded.last_attempt_at, last_error = excluded.last_error",
                (checkpoint_key, task_name, _utc_iso(attempted_at), error[:1000]),
            )

    def recent_runs(self, limit: int = 10) -> list[RunCheckpoint]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT run_id, scheduled_at, started_at, completed_at, status, "
                "gap_seconds, summary_json FROM run_checkpoints "
                "ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            RunCheckpoint(
                run_id=row["run_id"],
                scheduled_at=_required_datetime(row["scheduled_at"]),
                started_at=_required_datetime(row["started_at"]),
                completed_at=_parse_datetime(row["completed_at"]),
                status=row["status"],
                gap_seconds=row["gap_seconds"],
                summary=json.loads(row["summary_json"]),
            )
            for row in rows
        ]


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("runtime timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _required_datetime(value: str) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError("required runtime timestamp is missing")
    return parsed
