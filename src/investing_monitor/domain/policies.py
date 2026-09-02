from __future__ import annotations

from dataclasses import dataclass, replace
from math import floor

from .models import (
    Direction,
    MarketSnapshot,
    PriceBandSignal,
    PriceBandState,
    RelativeOutcome,
    VolumeSnapshot,
)


@dataclass(frozen=True)
class RelativeAssessment:
    benchmark: RelativeOutcome
    peers: RelativeOutcome
    peer_symbols: tuple[str, ...]
    peer_average_change_pct: float | None


@dataclass(frozen=True)
class VolumeAssessment:
    ratio: float | None
    is_ready: bool
    is_exploded: bool


class PriceBandPolicy:
    def __init__(self, start_level: int = 4, step: int = 1) -> None:
        if start_level < 1 or step < 1:
            raise ValueError("start_level and step must be positive")
        self.start_level = start_level
        self.step = step

    def evaluate(
        self,
        snapshot: MarketSnapshot,
        state: PriceBandState | None,
    ) -> tuple[PriceBandSignal | None, PriceBandState]:
        state = self._state_for(snapshot, state)
        direction = snapshot.direction
        level = floor(abs(snapshot.change_pct))
        if direction is Direction.FLAT or level < self.start_level:
            return None, state

        if direction is Direction.UP:
            previous = state.upward_high_watermark
            opposite_seen = state.downward_high_watermark >= self.start_level
        else:
            previous = state.downward_high_watermark
            opposite_seen = state.upward_high_watermark >= self.start_level

        normalized_level = self.start_level + (
            (level - self.start_level) // self.step
        ) * self.step
        if normalized_level <= previous:
            return None, state

        if direction is Direction.UP:
            next_state = replace(state, upward_high_watermark=normalized_level)
        else:
            next_state = replace(state, downward_high_watermark=normalized_level)

        direction_token = "up" if direction is Direction.UP else "down"
        event_key = (
            f"{snapshot.ticker.upper()}:{snapshot.trading_date.isoformat()}:"
            f"price-band:{direction_token}:{normalized_level}"
        )
        signal = PriceBandSignal(
            event_key=event_key,
            ticker=snapshot.ticker.upper(),
            trading_date=snapshot.trading_date,
            direction=direction,
            level=normalized_level,
            is_reversal=opposite_seen,
            observed_at=snapshot.observed_at,
        )
        return signal, next_state

    @staticmethod
    def _state_for(
        snapshot: MarketSnapshot,
        state: PriceBandState | None,
    ) -> PriceBandState:
        if state is None or state.trading_date != snapshot.trading_date:
            return PriceBandState(trading_date=snapshot.trading_date)
        return state


def assess_relative_performance(
    snapshot: MarketSnapshot,
    *,
    neutral_band_pct: float = 0.5,
    minimum_peers: int = 2,
) -> RelativeAssessment:
    benchmark = _relative_outcome(
        snapshot.change_pct,
        snapshot.benchmark_change_pct,
        neutral_band_pct,
    )
    valid_peers = tuple(
        sorted(
            (symbol.upper(), float(change))
            for symbol, change in snapshot.peer_changes.items()
            if change is not None
        )
    )
    if len(valid_peers) < minimum_peers:
        return RelativeAssessment(
            benchmark=benchmark,
            peers=RelativeOutcome.UNAVAILABLE,
            peer_symbols=tuple(symbol for symbol, _ in valid_peers),
            peer_average_change_pct=None,
        )

    peer_average = sum(change for _, change in valid_peers) / len(valid_peers)
    return RelativeAssessment(
        benchmark=benchmark,
        peers=_relative_outcome(snapshot.change_pct, peer_average, neutral_band_pct),
        peer_symbols=tuple(symbol for symbol, _ in valid_peers),
        peer_average_change_pct=peer_average,
    )


def assess_intraday_volume(
    volume: VolumeSnapshot | None,
    *,
    minimum_sessions: int = 10,
    explosion_ratio: float = 1.5,
) -> VolumeAssessment:
    if volume is None or volume.baseline_sessions < minimum_sessions:
        return VolumeAssessment(ratio=None, is_ready=False, is_exploded=False)
    ratio = volume.ratio
    if ratio is None:
        return VolumeAssessment(ratio=None, is_ready=False, is_exploded=False)
    return VolumeAssessment(
        ratio=ratio,
        is_ready=True,
        is_exploded=ratio >= explosion_ratio,
    )


def _relative_outcome(
    actual_change_pct: float,
    comparison_change_pct: float | None,
    neutral_band_pct: float,
) -> RelativeOutcome:
    if comparison_change_pct is None:
        return RelativeOutcome.UNAVAILABLE
    difference = actual_change_pct - comparison_change_pct
    if difference > neutral_band_pct:
        return RelativeOutcome.OUTPERFORM
    if difference < -neutral_band_pct:
        return RelativeOutcome.UNDERPERFORM
    return RelativeOutcome.INLINE
