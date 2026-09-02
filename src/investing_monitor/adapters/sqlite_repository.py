from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from investing_monitor.domain.models import PriceBandSignal, PriceBandState
from investing_monitor.ports.repository import PendingDelivery


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
    FOREIGN KEY(event_key) REFERENCES alerts(event_key)
);
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
        with self._connect() as connection:
            connection.executescript(SCHEMA)

    def load_price_band_state(self, ticker: str) -> PriceBandState | None:
        with self._connect() as connection:
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
        with self._connect() as connection:
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
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id, event_key, payload_json, attempts FROM outbox "
                "WHERE delivered_at IS NULL AND next_attempt_at <= ? "
                "ORDER BY id LIMIT ?",
                (now.astimezone(timezone.utc).isoformat(), limit),
            ).fetchall()
        return [
            PendingDelivery(
                outbox_id=row["id"],
                event_key=row["event_key"],
                payload=json.loads(row["payload_json"]),
                attempts=row["attempts"],
            )
            for row in rows
        ]

    def mark_delivered(self, outbox_id: int, delivered_at: datetime, receipt: str = "") -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE outbox SET delivered_at = ?, receipt = ?, last_error = '' WHERE id = ?",
                (delivered_at.astimezone(timezone.utc).isoformat(), receipt, outbox_id),
            )

    def mark_failed(self, outbox_id: int, next_attempt_at: datetime, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE outbox SET attempts = attempts + 1, next_attempt_at = ?, last_error = ? "
                "WHERE id = ?",
                (next_attempt_at.astimezone(timezone.utc).isoformat(), error[:1000], outbox_id),
            )
