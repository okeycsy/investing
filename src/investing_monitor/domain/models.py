from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Mapping


class MarketSession(str, Enum):
    PRE = "pre"
    REGULAR = "regular"
    POST = "post"
    CLOSED = "closed"


class Direction(str, Enum):
    UP = "up"
    DOWN = "down"
    FLAT = "flat"


class RelativeOutcome(str, Enum):
    OUTPERFORM = "outperform"
    UNDERPERFORM = "underperform"
    INLINE = "inline"
    UNAVAILABLE = "unavailable"


class ThesisImpact(str, Enum):
    STRENGTHEN = "strengthen"
    NEUTRAL = "neutral"
    RISK = "risk"
    DAMAGE = "damage"


@dataclass(frozen=True)
class InstrumentProfile:
    ticker: str
    benchmark: str
    peers: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.ticker.strip():
            raise ValueError("ticker is required")
        if not self.benchmark.strip():
            raise ValueError("benchmark is required")
        normalized_peers = tuple(dict.fromkeys(peer.strip().upper() for peer in self.peers if peer.strip()))
        if self.ticker.upper() in normalized_peers:
            raise ValueError("the monitored ticker cannot be its own peer")
        object.__setattr__(self, "ticker", self.ticker.strip().upper())
        object.__setattr__(self, "benchmark", self.benchmark.strip().upper())
        object.__setattr__(self, "peers", normalized_peers)


@dataclass(frozen=True)
class MarketSnapshot:
    ticker: str
    trading_date: date
    observed_at: datetime
    session: MarketSession
    change_pct: float
    benchmark_change_pct: float | None = None
    benchmark_symbol: str = "SOXX"
    peer_changes: Mapping[str, float | None] = field(default_factory=dict)

    @property
    def direction(self) -> Direction:
        if self.change_pct > 0:
            return Direction.UP
        if self.change_pct < 0:
            return Direction.DOWN
        return Direction.FLAT


@dataclass(frozen=True)
class MarketFrame:
    snapshot: MarketSnapshot
    close_price: float
    reference_close: float
    cumulative_volume: int = 0

    def __post_init__(self) -> None:
        if self.snapshot.observed_at.tzinfo is None:
            raise ValueError("market frame timestamp must be timezone-aware")
        if self.close_price <= 0 or self.reference_close <= 0:
            raise ValueError("market frame prices must be positive")
        if self.cumulative_volume < 0:
            raise ValueError("cumulative volume cannot be negative")


@dataclass(frozen=True)
class MarketCycle:
    ticker: str
    trading_date: date
    frames: tuple[MarketFrame, ...]
    volume: VolumeSnapshot | None
    source_age_seconds: int

    @property
    def latest_snapshot(self) -> MarketSnapshot | None:
        return self.frames[-1].snapshot if self.frames else None

    @property
    def replayed_frames(self) -> int:
        return max(0, len(self.frames) - 1)


@dataclass(frozen=True)
class VolumeSnapshot:
    observed_volume: int
    expected_volume: int
    baseline_sessions: int
    lookback_sessions: int = 20

    @property
    def ratio(self) -> float | None:
        if self.expected_volume <= 0:
            return None
        return self.observed_volume / self.expected_volume


@dataclass(frozen=True)
class CloseMarketContext:
    snapshot: MarketSnapshot
    volume: VolumeSnapshot | None


@dataclass(frozen=True)
class Catalyst:
    canonical_id: str
    headline: str
    summary: str
    source_name: str
    source_url: str
    published_at: datetime
    impact: ThesisImpact = ThesisImpact.NEUTRAL
    confidence: str = "medium"
    facts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.canonical_id:
            raise ValueError("canonical_id is required")
        if not self.headline or not self.summary:
            raise ValueError("headline and summary are required")
        if not self.source_url.startswith(("https://", "http://")):
            raise ValueError("a traceable source_url is required")


@dataclass(frozen=True)
class PriceBandState:
    trading_date: date
    upward_high_watermark: int = 0
    downward_high_watermark: int = 0
    volume_alerted: bool = False


@dataclass(frozen=True)
class PriceBandSignal:
    event_key: str
    ticker: str
    trading_date: date
    direction: Direction
    level: int
    is_reversal: bool
    observed_at: datetime
    session: MarketSession = MarketSession.REGULAR


@dataclass(frozen=True)
class VolumeSignal:
    event_key: str
    ticker: str
    trading_date: date
    observed_at: datetime
