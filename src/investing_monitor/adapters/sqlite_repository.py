from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from investing_monitor.domain.evidence import (
    AnalyzedEvidence,
    CandidateDecision,
    EvidenceAnalysis,
    EvidenceCandidate,
    EvidenceDisposition,
    EvidenceKind,
    EvidenceSourceTier,
    EvidenceStatus,
    GroundedFact,
    candidate_identity,
)
from investing_monitor.domain.evidence_qualification import (
    evidence_disposition,
    legacy_evidence_qualification,
)
from investing_monitor.domain.models import (
    Catalyst,
    CloseMarketContext,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
    OfficialEvent,
    PriceBandSignal,
    PriceBandState,
    ThesisImpact,
    VolumeSnapshot,
)
from investing_monitor.ports.repository import (
    AlertRecord,
    EvidenceQualityRecord,
    MarketObservationRecord,
    PendingDelivery,
)
from investing_monitor.ports.runtime import RunCheckpoint, TaskCheckpoint
from investing_monitor.presentation.quality import require_valid_message


SCHEMA_VERSION = 7

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_sessions (
    ticker TEXT PRIMARY KEY,
    trading_date TEXT NOT NULL,
    upward_high_watermark INTEGER NOT NULL DEFAULT 0,
    downward_high_watermark INTEGER NOT NULL DEFAULT 0,
    volume_alerted INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_observations (
    ticker TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    session TEXT NOT NULL,
    close_price REAL NOT NULL,
    reference_close REAL NOT NULL,
    change_pct REAL NOT NULL,
    benchmark_symbol TEXT NOT NULL DEFAULT '',
    benchmark_change_pct REAL,
    peer_changes_json TEXT NOT NULL DEFAULT '{}',
    cumulative_volume INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(ticker, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_market_observations_ticker_time
ON market_observations(ticker, observed_at DESC);

CREATE TABLE IF NOT EXISTS market_volume_observations (
    ticker TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    observed_volume INTEGER NOT NULL,
    expected_volume INTEGER NOT NULL,
    baseline_sessions INTEGER NOT NULL,
    lookback_sessions INTEGER NOT NULL,
    PRIMARY KEY(ticker, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_market_volume_ticker_date
ON market_volume_observations(ticker, trading_date, observed_at DESC);

CREATE TABLE IF NOT EXISTS evidence_candidates (
    candidate_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    headline TEXT NOT NULL DEFAULT '',
    source_name TEXT NOT NULL DEFAULT '',
    source_url TEXT NOT NULL DEFAULT '',
    published_at TEXT,
    source_text TEXT NOT NULL DEFAULT '',
    external_id TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    cluster_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    status_reason TEXT NOT NULL DEFAULT '',
    analysis_json TEXT NOT NULL DEFAULT '{}',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_evidence_candidates_pending
ON evidence_candidates(status, next_attempt_at, published_at);

CREATE INDEX IF NOT EXISTS idx_evidence_candidates_cluster
ON evidence_candidates(cluster_key, published_at);

CREATE TABLE IF NOT EXISTS source_baselines (
    source_key TEXT PRIMARY KEY,
    initialized_at TEXT NOT NULL,
    candidate_count INTEGER NOT NULL DEFAULT 0
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
            self._ensure_column(
                connection,
                "market_sessions",
                "volume_alerted",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                connection,
                "market_observations",
                "benchmark_symbol",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "market_observations",
                "benchmark_change_pct",
                "REAL",
            )
            self._ensure_column(
                connection,
                "market_observations",
                "peer_changes_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
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
                "SELECT trading_date, upward_high_watermark, downward_high_watermark, "
                "volume_alerted "
                "FROM market_sessions WHERE ticker = ?",
                (ticker.upper(),),
            ).fetchone()
        if row is None:
            return None
        return PriceBandState(
            trading_date=date.fromisoformat(row["trading_date"]),
            upward_high_watermark=row["upward_high_watermark"],
            downward_high_watermark=row["downward_high_watermark"],
            volume_alerted=bool(row["volume_alerted"]),
        )

    def record_price_signal(
        self,
        signal: PriceBandSignal,
        state: PriceBandState,
        payload: dict,
    ) -> bool:
        require_valid_message("price_band", payload)
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        observed_at = _utc_iso(signal.observed_at)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            inserted = connection.execute(
                "INSERT OR IGNORE INTO alerts "
                "(event_key, ticker, alert_type, created_at, payload_json) "
                "VALUES (?, ?, 'price_band', ?, ?)",
                (signal.event_key, signal.ticker, observed_at, payload_json),
            ).rowcount
            if not inserted:
                return False
            connection.execute(
                "INSERT INTO market_sessions "
                "(ticker, trading_date, upward_high_watermark, downward_high_watermark, "
                "volume_alerted, updated_at) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(ticker) DO UPDATE SET "
                "trading_date = excluded.trading_date, "
                "upward_high_watermark = excluded.upward_high_watermark, "
                "downward_high_watermark = excluded.downward_high_watermark, "
                "volume_alerted = excluded.volume_alerted, "
                "updated_at = excluded.updated_at",
                (
                    signal.ticker,
                    state.trading_date.isoformat(),
                    state.upward_high_watermark,
                    state.downward_high_watermark,
                    int(state.volume_alerted),
                    observed_at,
                ),
            )
            connection.execute(
                "INSERT INTO outbox (event_key, payload_json, next_attempt_at) VALUES (?, ?, ?)",
                (signal.event_key, payload_json, observed_at),
            )
        return True

    def latest_market_observation_at(self, ticker: str) -> datetime | None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT observed_at FROM market_observations "
                "WHERE ticker = ? ORDER BY observed_at DESC LIMIT 1",
                (ticker.upper(),),
            ).fetchone()
        return _parse_datetime(row["observed_at"]) if row else None

    def record_market_cycle(
        self,
        ticker: str,
        state: PriceBandState,
        frames: Sequence[MarketFrame],
        volume: VolumeSnapshot | None,
        alerts: Sequence[AlertRecord],
        *,
        enqueue: bool = True,
    ) -> tuple[str, ...]:
        for alert in alerts:
            require_valid_message(alert.alert_type, alert.payload)
        ticker = ticker.upper()
        updated_at = (
            _utc_iso(frames[-1].snapshot.observed_at)
            if frames
            else datetime.now(timezone.utc).isoformat()
        )
        inserted_events: list[str] = []
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            for frame in frames:
                snapshot = frame.snapshot
                connection.execute(
                    "INSERT OR IGNORE INTO market_observations "
                    "(ticker, observed_at, trading_date, session, close_price, "
                    "reference_close, change_pct, benchmark_symbol, "
                    "benchmark_change_pct, peer_changes_json, cumulative_volume) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ticker,
                        _utc_iso(snapshot.observed_at),
                        snapshot.trading_date.isoformat(),
                        snapshot.session.value,
                        frame.close_price,
                        frame.reference_close,
                        snapshot.change_pct,
                        snapshot.benchmark_symbol,
                        snapshot.benchmark_change_pct,
                        json.dumps(
                            dict(snapshot.peer_changes),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        frame.cumulative_volume,
                    ),
                )
            if volume is not None and frames:
                connection.execute(
                    "INSERT INTO market_volume_observations "
                    "(ticker, observed_at, trading_date, observed_volume, "
                    "expected_volume, baseline_sessions, lookback_sessions) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(ticker, observed_at) DO UPDATE SET "
                    "observed_volume = excluded.observed_volume, "
                    "expected_volume = excluded.expected_volume, "
                    "baseline_sessions = excluded.baseline_sessions, "
                    "lookback_sessions = excluded.lookback_sessions",
                    (
                        ticker,
                        _utc_iso(frames[-1].snapshot.observed_at),
                        frames[-1].snapshot.trading_date.isoformat(),
                        volume.observed_volume,
                        volume.expected_volume,
                        volume.baseline_sessions,
                        volume.lookback_sessions,
                    ),
                )
            connection.execute(
                "INSERT INTO market_sessions "
                "(ticker, trading_date, upward_high_watermark, downward_high_watermark, "
                "volume_alerted, updated_at) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(ticker) DO UPDATE SET "
                "trading_date = excluded.trading_date, "
                "upward_high_watermark = excluded.upward_high_watermark, "
                "downward_high_watermark = excluded.downward_high_watermark, "
                "volume_alerted = excluded.volume_alerted, "
                "updated_at = excluded.updated_at",
                (
                    ticker,
                    state.trading_date.isoformat(),
                    state.upward_high_watermark,
                    state.downward_high_watermark,
                    int(state.volume_alerted),
                    updated_at,
                ),
            )
            for alert in alerts:
                payload_json = json.dumps(
                    alert.payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO alerts "
                    "(event_key, ticker, alert_type, created_at, payload_json) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        alert.event_key,
                        alert.ticker.upper(),
                        alert.alert_type,
                        _utc_iso(alert.created_at),
                        payload_json,
                    ),
                ).rowcount
                if not inserted:
                    continue
                if enqueue:
                    connection.execute(
                        "INSERT INTO outbox (event_key, payload_json, next_attempt_at) "
                        "VALUES (?, ?, ?)",
                        (alert.event_key, payload_json, updated_at),
                    )
                inserted_events.append(alert.event_key)
        return tuple(inserted_events)

    def load_close_market_context(
        self,
        ticker: str,
        trading_date: date,
    ) -> CloseMarketContext | None:
        ticker = ticker.upper()
        with closing(self._connect()) as connection, connection:
            market_row = connection.execute(
                "SELECT ticker, trading_date, observed_at, session, change_pct, "
                "benchmark_symbol, benchmark_change_pct, peer_changes_json "
                "FROM market_observations WHERE ticker = ? AND trading_date = ? "
                "ORDER BY CASE WHEN session = 'regular' THEN 0 ELSE 1 END, "
                "observed_at DESC LIMIT 1",
                (ticker, trading_date.isoformat()),
            ).fetchone()
            volume_row = connection.execute(
                "SELECT observed_volume, expected_volume, baseline_sessions, "
                "lookback_sessions FROM market_volume_observations "
                "WHERE ticker = ? AND trading_date = ? "
                "ORDER BY observed_at DESC LIMIT 1",
                (ticker, trading_date.isoformat()),
            ).fetchone()
        if market_row is None:
            return None
        snapshot = MarketSnapshot(
            ticker=market_row["ticker"],
            trading_date=date.fromisoformat(market_row["trading_date"]),
            observed_at=_required_datetime(market_row["observed_at"]),
            session=MarketSession(market_row["session"]),
            change_pct=market_row["change_pct"],
            benchmark_change_pct=market_row["benchmark_change_pct"],
            benchmark_symbol=market_row["benchmark_symbol"],
            peer_changes=json.loads(market_row["peer_changes_json"]),
        )
        volume = None
        if volume_row is not None:
            volume = VolumeSnapshot(
                observed_volume=volume_row["observed_volume"],
                expected_volume=volume_row["expected_volume"],
                baseline_sessions=volume_row["baseline_sessions"],
                lookback_sessions=volume_row["lookback_sessions"],
            )
        return CloseMarketContext(snapshot=snapshot, volume=volume)

    def load_close_market_contexts(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> tuple[CloseMarketContext, ...]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT DISTINCT trading_date FROM market_observations "
                "WHERE ticker = ? AND trading_date BETWEEN ? AND ? "
                "AND session = 'regular' ORDER BY trading_date",
                (ticker.upper(), start_date.isoformat(), end_date.isoformat()),
            ).fetchall()
        contexts = [
            self.load_close_market_context(ticker, date.fromisoformat(row["trading_date"]))
            for row in rows
        ]
        return tuple(context for context in contexts if context is not None)

    def record_alert(self, alert: AlertRecord, *, enqueue: bool = True) -> bool:
        require_valid_message(alert.alert_type, alert.payload)
        payload_json = json.dumps(
            alert.payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            inserted = bool(
                connection.execute(
                    "INSERT OR IGNORE INTO alerts "
                    "(event_key, ticker, alert_type, created_at, payload_json) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        alert.event_key,
                        alert.ticker.upper(),
                        alert.alert_type,
                        _utc_iso(alert.created_at),
                        payload_json,
                    ),
                ).rowcount
            )
            if inserted and enqueue:
                connection.execute(
                    "INSERT INTO outbox (event_key, payload_json, next_attempt_at) "
                    "VALUES (?, ?, ?)",
                    (alert.event_key, payload_json, _utc_iso(alert.created_at)),
                )
        return inserted

    def recent_alerts(self, limit: int = 100) -> list[AlertRecord]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT event_key, ticker, alert_type, created_at, payload_json "
                "FROM alerts ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            AlertRecord(
                event_key=row["event_key"],
                ticker=row["ticker"],
                alert_type=row["alert_type"],
                created_at=_required_datetime(row["created_at"]),
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    def recent_market_observations(
        self,
        limit_days: int = 10,
    ) -> list[MarketObservationRecord]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT ticker, trading_date, observed_at, session "
                "FROM market_observations WHERE trading_date IN ("
                "SELECT trading_date FROM market_observations "
                "GROUP BY trading_date ORDER BY trading_date DESC LIMIT ?"
                ") ORDER BY observed_at",
                (max(1, limit_days),),
            ).fetchall()
        return [
            MarketObservationRecord(
                ticker=row["ticker"],
                trading_date=date.fromisoformat(row["trading_date"]),
                observed_at=_required_datetime(row["observed_at"]),
                session=row["session"],
            )
            for row in rows
        ]

    def recent_evidence_quality_records(
        self,
        limit: int = 500,
    ) -> list[EvidenceQualityRecord]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT candidate_id, source_url, source_kind, status, status_reason, "
                "cluster_key, analysis_json FROM evidence_candidates "
                "ORDER BY last_seen_at DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
        records = []
        for row in rows:
            payload = json.loads(row["analysis_json"] or "{}")
            relevant = payload.get("relevant") if isinstance(payload, dict) else None
            analysis = _analysis_from_payload(payload)
            source_kind = row["source_kind"]
            disposition = evidence_disposition(
                EvidenceKind(source_kind),
                analysis,
            )
            records.append(
                EvidenceQualityRecord(
                    candidate_id=row["candidate_id"],
                    source_url=row["source_url"],
                    status=row["status"],
                    status_reason=row["status_reason"],
                    cluster_key=row["cluster_key"],
                    relevant=relevant if isinstance(relevant, bool) else None,
                    source_kind=source_kind,
                    event_type=analysis.event_type,
                    materiality=analysis.materiality,
                    alert_disposition=disposition.value,
                )
            )
        return records

    def quality_status_counts(self) -> dict[str, dict[str, int]]:
        with closing(self._connect()) as connection, connection:
            outbox_rows = connection.execute(
                "SELECT delivery_status, count(*) AS count FROM outbox "
                "GROUP BY delivery_status"
            ).fetchall()
            evidence_rows = connection.execute(
                "SELECT status, count(*) AS count FROM evidence_candidates "
                "GROUP BY status"
            ).fetchall()
        return {
            "outbox": {row["delivery_status"]: row["count"] for row in outbox_rows},
            "evidence": {row["status"]: row["count"] for row in evidence_rows},
        }

    def record_evidence_decisions(
        self,
        decisions: Sequence[CandidateDecision],
        cluster_keys: dict[str, str],
        seen_at: datetime,
    ) -> tuple[str, ...]:
        seen_at_iso = _utc_iso(seen_at)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            inserted_pending = _insert_evidence_decisions(
                connection,
                decisions,
                cluster_keys,
                seen_at_iso,
            )
        return inserted_pending

    def has_source_baseline(self, source_key: str) -> bool:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT 1 FROM source_baselines WHERE source_key = ?",
                (source_key,),
            ).fetchone()
        return row is not None

    def record_evidence_baseline(
        self,
        source_key: str,
        decisions: Sequence[CandidateDecision],
        seen_at: datetime,
    ) -> int:
        seen_at_iso = _utc_iso(seen_at)
        baseline_decisions = tuple(
            CandidateDecision(
                status=EvidenceStatus.BASELINE,
                reason=f"initial source baseline: {source_key}",
                candidate=decision.candidate,
                raw=decision.raw,
            )
            if decision.candidate is not None
            else decision
            for decision in decisions
        )
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            before = connection.total_changes
            _insert_evidence_decisions(
                connection,
                baseline_decisions,
                {},
                seen_at_iso,
            )
            inserted = connection.total_changes - before
            connection.execute(
                "INSERT OR IGNORE INTO source_baselines "
                "(source_key, initialized_at, candidate_count) VALUES (?, ?, ?)",
                (source_key, seen_at_iso, inserted),
            )
        return inserted

    def pending_evidence_candidates(
        self,
        now: datetime,
        limit: int = 20,
    ) -> list[EvidenceCandidate]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT candidate_id, ticker, source_kind, headline, source_name, "
                "source_url, published_at, source_text, external_id, metadata_json "
                "FROM evidence_candidates "
                "WHERE status IN ('pending', 'failed') "
                "AND next_attempt_at <= ? "
                "ORDER BY published_at, first_seen_at LIMIT ?",
                (_utc_iso(now), limit),
            ).fetchall()
        return [
            EvidenceCandidate(
                candidate_id=row["candidate_id"],
                ticker=row["ticker"],
                kind=EvidenceKind(row["source_kind"]),
                headline=row["headline"],
                source_name=row["source_name"],
                source_url=row["source_url"],
                published_at=_required_datetime(row["published_at"]),
                source_text=row["source_text"],
                external_id=row["external_id"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def update_evidence_source_text(
        self,
        candidate_id: str,
        source_text: str,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            if metadata is None:
                connection.execute(
                    "UPDATE evidence_candidates SET source_text = ? WHERE candidate_id = ?",
                    (source_text, candidate_id),
                )
            else:
                connection.execute(
                    "UPDATE evidence_candidates SET source_text = ?, metadata_json = ? "
                    "WHERE candidate_id = ?",
                    (
                        source_text,
                        json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
                        candidate_id,
                    ),
                )

    def mark_evidence_filtered(
        self,
        candidate_id: str,
        filtered_at: datetime,
        reason: str,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE evidence_candidates SET status = 'filtered', status_reason = ?, "
                "last_attempt_at = ?, next_attempt_at = NULL, last_error = '' "
                "WHERE candidate_id = ?",
                (reason, _utc_iso(filtered_at), candidate_id),
            )

    def evidence_cluster_key(self, candidate_id: str) -> str:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT ticker, cluster_key FROM evidence_candidates "
                "WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            return ""
        return row["cluster_key"] or f"{row['ticker']}:evidence:{candidate_id}"

    def link_evidence_cluster(self, candidate_id: str, cluster_key: str) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE evidence_candidates SET cluster_key = ? WHERE candidate_id = ?",
                (cluster_key, candidate_id),
            )

    def recent_analyzed_evidence(
        self,
        ticker: str,
        since: datetime,
        limit: int = 30,
    ) -> list[AnalyzedEvidence]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT candidate_id, ticker, source_kind, headline, source_name, "
                "source_url, published_at, source_text, external_id, metadata_json, "
                "cluster_key, analysis_json FROM evidence_candidates "
                "WHERE ticker = ? AND status = 'analyzed' AND published_at >= ? "
                "AND json_extract(analysis_json, '$.relevant') = 1 "
                "ORDER BY published_at DESC LIMIT ?",
                (ticker.upper(), _utc_iso(since), limit),
            ).fetchall()
        return [
            AnalyzedEvidence(
                candidate=_candidate_from_row(row),
                analysis=_analysis_from_payload(json.loads(row["analysis_json"])),
                cluster_key=row["cluster_key"] or row["candidate_id"],
            )
            for row in rows
        ]

    def record_evidence_analysis(
        self,
        candidate_id: str,
        analysis: EvidenceAnalysis,
        analyzed_at: datetime,
        alert: AlertRecord | None = None,
        *,
        enqueue: bool = True,
    ) -> bool:
        if alert is not None:
            require_valid_message(alert.alert_type, alert.payload)
        payload = _analysis_payload(analysis)
        analyzed_at_iso = _utc_iso(analyzed_at)
        inserted_alert = False
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE evidence_candidates SET status = 'analyzed', status_reason = ?, "
                "analysis_json = ?, last_attempt_at = ?, next_attempt_at = NULL, "
                "attempt_count = attempt_count + 1, last_error = '' WHERE candidate_id = ?",
                (
                    "relevant" if analysis.relevant else "not relevant",
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    analyzed_at_iso,
                    candidate_id,
                ),
            )
            if alert is not None:
                alert_payload = json.dumps(
                    alert.payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                inserted_alert = bool(
                    connection.execute(
                        "INSERT OR IGNORE INTO alerts "
                        "(event_key, ticker, alert_type, created_at, payload_json) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            alert.event_key,
                            alert.ticker.upper(),
                            alert.alert_type,
                            _utc_iso(alert.created_at),
                            alert_payload,
                        ),
                    ).rowcount
                )
                if inserted_alert and enqueue:
                    connection.execute(
                        "INSERT INTO outbox (event_key, payload_json, next_attempt_at) "
                        "VALUES (?, ?, ?)",
                        (alert.event_key, alert_payload, analyzed_at_iso),
                    )
        return inserted_alert

    def suppress_pending_deliveries(
        self,
        suppressed_at: datetime,
        reason: str,
    ) -> int:
        with closing(self._connect()) as connection, connection:
            return connection.execute(
                "UPDATE outbox SET delivery_status = 'suppressed', attempted_at = ?, "
                "last_error = ? WHERE delivered_at IS NULL "
                "AND delivery_status IN ('pending', 'failed')",
                (_utc_iso(suppressed_at), reason[:1000]),
            ).rowcount

    def recent_catalysts(
        self,
        ticker: str,
        since: datetime,
        limit: int = 2,
    ) -> list[Catalyst]:
        cluster_lookback = since - timedelta(days=7)
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT candidate_id, cluster_key, headline, source_name, source_url, "
                "published_at, source_kind, analysis_json FROM evidence_candidates "
                "WHERE ticker = ? AND status = 'analyzed' AND published_at >= ? "
                "AND json_extract(analysis_json, '$.relevant') = 1 "
                "AND COALESCE(json_extract(metadata_json, '$.calendar_only'), 0) = 0 "
                "ORDER BY published_at DESC LIMIT 100",
                (ticker.upper(), _utc_iso(cluster_lookback)),
            ).fetchall()
        grouped: dict[str, list[Catalyst]] = {}
        for row in rows:
            payload = json.loads(row["analysis_json"])
            analysis = _analysis_from_payload(payload)
            if (
                evidence_disposition(EvidenceKind(row["source_kind"]), analysis)
                is EvidenceDisposition.LEDGER
            ):
                continue
            catalyst = Catalyst(
                canonical_id=row["cluster_key"] or row["candidate_id"],
                headline=payload["headline_ko"],
                summary=payload["summary_ko"],
                source_name=row["source_name"],
                source_url=row["source_url"],
                published_at=_required_datetime(row["published_at"]),
                impact=ThesisImpact(payload["thesis_impact"]),
                confidence=payload["confidence"],
                facts=tuple(
                    fact["fact_ko"] for fact in payload.get("facts") or []
                ),
                source_kind=row["source_kind"],
            )
            grouped.setdefault(catalyst.canonical_id, []).append(catalyst)
        catalysts = []
        for candidates in grouped.values():
            event_started_at = min(item.published_at for item in candidates)
            if event_started_at < since:
                continue
            catalysts.append(
                max(
                    candidates,
                    key=lambda item: (
                        _catalyst_source_rank(item.source_kind, item.source_name),
                        -item.published_at.timestamp(),
                    ),
                )
            )
        impact_rank = {
            ThesisImpact.DAMAGE: 4,
            ThesisImpact.RISK: 3,
            ThesisImpact.STRENGTHEN: 2,
            ThesisImpact.NEUTRAL: 1,
        }
        catalysts.sort(
            key=lambda item: (impact_rank[item.impact], item.published_at),
            reverse=True,
        )
        return catalysts[:limit]

    def upcoming_official_events(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        limit: int = 5,
    ) -> list[OfficialEvent]:
        with closing(self._connect()) as connection, connection:
            rows = connection.execute(
                "SELECT source_url, analysis_json FROM evidence_candidates "
                "WHERE ticker = ? AND source_kind = 'ir' AND status = 'analyzed' "
                "ORDER BY published_at DESC",
                (ticker.upper(),),
            ).fetchall()
        events = {}
        for row in rows:
            payload = json.loads(row["analysis_json"])
            for item in payload.get("official_events") or []:
                try:
                    event_date = date.fromisoformat(item["date"])
                    event = OfficialEvent(
                        event_date=event_date,
                        title_ko=item["title_ko"],
                        source_url=row["source_url"],
                        source_text=item["source_text"],
                        time_et=item.get("time_et", ""),
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                if start_date <= event.event_date <= end_date:
                    key = (event.event_date, event.title_ko.casefold())
                    events.setdefault(key, event)
        return sorted(
            events.values(),
            key=lambda item: (item.event_date, item.time_et, item.title_ko),
        )[:limit]

    def mark_evidence_analyzed(
        self,
        candidate_id: str,
        analysis: EvidenceAnalysis,
        analyzed_at: datetime,
    ) -> None:
        self.record_evidence_analysis(candidate_id, analysis, analyzed_at)

    def mark_evidence_failed(
        self,
        candidate_id: str,
        attempted_at: datetime,
        next_attempt_at: datetime,
        error: str,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE evidence_candidates SET status = 'failed', last_attempt_at = ?, "
                "next_attempt_at = ?, attempt_count = attempt_count + 1, last_error = ? "
                "WHERE candidate_id = ?",
                (
                    _utc_iso(attempted_at),
                    _utc_iso(next_attempt_at),
                    error[:1000],
                    candidate_id,
                ),
            )

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

    def mark_discarded(
        self,
        outbox_id: int,
        attempted_at: datetime,
        error: str,
    ) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "UPDATE outbox SET delivery_status = 'discarded', attempted_at = ?, "
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


def _catalyst_source_rank(source_kind: str, source_name: str) -> int:
    kind_rank = {"ir": 40, "sec": 30, "news": 10}.get(source_kind, 0)
    normalized = source_name.casefold()
    if any(name in normalized for name in ("pr newswire", "business wire")):
        return kind_rank + 3
    if any(name in normalized for name in ("reuters", "associated press")):
        return kind_rank + 2
    if "globenewswire" in normalized:
        return kind_rank + 1
    return kind_rank


def _insert_evidence_decisions(
    connection: sqlite3.Connection,
    decisions: Sequence[CandidateDecision],
    cluster_keys: Mapping[str, str],
    seen_at_iso: str,
) -> tuple[str, ...]:
    inserted_pending: list[str] = []
    for decision in decisions:
        candidate = decision.candidate
        raw = decision.raw
        if candidate is not None:
            candidate_id = candidate.candidate_id
            ticker = candidate.ticker.upper()
            source_kind = candidate.kind.value
            headline = candidate.headline
            source_name = candidate.source_name
            source_url = candidate.source_url
            published_at = _utc_iso(candidate.published_at)
            source_text = candidate.source_text
            external_id = candidate.external_id
            metadata = dict(candidate.metadata)
        elif raw is not None:
            candidate_id = candidate_identity(raw)
            ticker = raw.ticker.upper()
            source_kind = raw.kind.value
            headline = raw.headline
            source_name = raw.source_name
            source_url = raw.source_url
            published_at = (
                _utc_iso(raw.published_at)
                if raw.published_at is not None and raw.published_at.tzinfo
                else None
            )
            source_text = raw.source_text
            external_id = raw.external_id
            metadata = dict(raw.metadata)
        else:
            continue
        inserted = connection.execute(
            "INSERT OR IGNORE INTO evidence_candidates "
            "(candidate_id, ticker, source_kind, headline, source_name, "
            "source_url, published_at, source_text, external_id, metadata_json, "
            "cluster_key, status, status_reason, first_seen_at, last_seen_at, "
            "next_attempt_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                candidate_id,
                ticker,
                source_kind,
                headline,
                source_name,
                source_url,
                published_at,
                source_text,
                external_id,
                json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
                cluster_keys.get(candidate_id, ""),
                decision.status.value,
                decision.reason,
                seen_at_iso,
                seen_at_iso,
                seen_at_iso if decision.status is EvidenceStatus.PENDING else None,
            ),
        ).rowcount
        if inserted and decision.status is EvidenceStatus.PENDING:
            inserted_pending.append(candidate_id)
        if not inserted:
            connection.execute(
                "UPDATE evidence_candidates SET last_seen_at = ?, "
                "cluster_key = CASE WHEN cluster_key = '' THEN ? ELSE cluster_key END "
                "WHERE candidate_id = ?",
                (seen_at_iso, cluster_keys.get(candidate_id, ""), candidate_id),
            )
    return tuple(inserted_pending)


def _analysis_payload(analysis: EvidenceAnalysis) -> dict[str, object]:
    return {
        "candidate_id": analysis.candidate_id,
        "relevant": analysis.relevant,
        "headline_ko": analysis.headline_ko,
        "summary_ko": analysis.summary_ko,
        "facts": [
            {"source_text": fact.source_text, "fact_ko": fact.fact_ko}
            for fact in analysis.facts
        ],
        "interpretation_ko": analysis.interpretation_ko,
        "thesis_impact": analysis.thesis_impact,
        "impact_reason_ko": analysis.impact_reason_ko,
        "confidence": analysis.confidence,
        "event_type": analysis.event_type,
        "company_directness": analysis.company_directness,
        "new_fact": analysis.new_fact,
        "materiality": analysis.materiality,
        "source_tier": analysis.source_tier,
        "alert_worthy": analysis.alert_worthy,
        "official_events": [
            {
                "title_ko": event.title_ko,
                "date": event.event_date.isoformat(),
                "time_et": event.time_et,
                "source_text": event.source_text,
                "source_url": event.source_url,
            }
            for event in analysis.official_events
        ],
    }


def _analysis_from_payload(payload: Mapping[str, object]) -> EvidenceAnalysis:
    qualification = legacy_evidence_qualification(payload)
    return EvidenceAnalysis(
        candidate_id=str(payload.get("candidate_id") or ""),
        relevant=bool(payload.get("relevant")),
        headline_ko=str(payload.get("headline_ko") or ""),
        summary_ko=str(payload.get("summary_ko") or ""),
        facts=tuple(
            GroundedFact(
                source_text=str(fact.get("source_text") or ""),
                fact_ko=str(fact.get("fact_ko") or ""),
            )
            for fact in payload.get("facts") or []
            if isinstance(fact, dict)
        ),
        interpretation_ko=str(payload.get("interpretation_ko") or ""),
        thesis_impact=str(payload.get("thesis_impact") or "neutral"),
        impact_reason_ko=str(payload.get("impact_reason_ko") or ""),
        confidence=str(payload.get("confidence") or "medium"),
        event_type=str(payload.get("event_type") or qualification["event_type"]),
        company_directness=_payload_bool(
            payload,
            "company_directness",
            qualification["company_directness"],
        ),
        new_fact=_payload_bool(payload, "new_fact", qualification["new_fact"]),
        materiality=str(payload.get("materiality") or qualification["materiality"]),
        source_tier=str(
            payload.get("source_tier") or EvidenceSourceTier.SECONDARY.value
        ),
        alert_worthy=_payload_bool(
            payload,
            "alert_worthy",
            qualification["alert_worthy"],
        ),
        official_events=tuple(
            OfficialEvent(
                event_date=date.fromisoformat(str(event.get("date") or "")),
                title_ko=str(event.get("title_ko") or ""),
                source_url=str(event.get("source_url") or "https://example.invalid"),
                source_text=str(event.get("source_text") or ""),
                time_et=str(event.get("time_et") or ""),
            )
            for event in payload.get("official_events") or []
            if isinstance(event, dict)
        ),
    )


def _payload_bool(
    payload: Mapping[str, object],
    key: str,
    default: bool,
) -> bool:
    value = payload.get(key)
    return value if isinstance(value, bool) else default


def _candidate_from_row(row: sqlite3.Row) -> EvidenceCandidate:
    return EvidenceCandidate(
        candidate_id=row["candidate_id"],
        ticker=row["ticker"],
        kind=EvidenceKind(row["source_kind"]),
        headline=row["headline"],
        source_name=row["source_name"],
        source_url=row["source_url"],
        published_at=_required_datetime(row["published_at"]),
        source_text=row["source_text"],
        external_id=row["external_id"],
        metadata=json.loads(row["metadata_json"]),
    )


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _required_datetime(value: str) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError("required runtime timestamp is missing")
    return parsed
