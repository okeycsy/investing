from __future__ import annotations

from dataclasses import dataclass, replace
from math import floor

from .models import (
    Direction,
    MarketSensitivity,
    MarketSnapshot,
    PriceBandSignal,
    PriceBandState,
    RelativeOutcome,
    SituationVerdict,
    VolumeSnapshot,
)


@dataclass(frozen=True)
class RelativeAssessment:
    benchmark: RelativeOutcome
    benchmark_symbol: str
    peers: RelativeOutcome
    peer_symbols: tuple[str, ...]
    peer_average_change_pct: float | None
    benchmark_normalized: bool = False
    peers_normalized: bool = False
    model_samples: int = 0


@dataclass(frozen=True)
class SituationAssessment:
    verdict: SituationVerdict
    confidence: str
    sensitivity_adjusted: bool


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
            session=snapshot.session,
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
    sensitivity: MarketSensitivity | None = None,
) -> RelativeAssessment:
    valid_model = _matching_sensitivity(snapshot, sensitivity)
    benchmark_beta = valid_model.benchmark_beta if valid_model else None
    benchmark_band = (
        valid_model.benchmark_residual_band_pct if valid_model else None
    )
    benchmark = _relative_outcome(
        snapshot.change_pct,
        snapshot.benchmark_change_pct,
        benchmark_band or neutral_band_pct,
        beta=benchmark_beta,
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
            benchmark_symbol=snapshot.benchmark_symbol.upper(),
            peers=RelativeOutcome.UNAVAILABLE,
            peer_symbols=tuple(symbol for symbol, _ in valid_peers),
            peer_average_change_pct=None,
            benchmark_normalized=benchmark_beta is not None,
            model_samples=(valid_model.benchmark_samples if valid_model else 0),
        )

    peer_average = sum(change for _, change in valid_peers) / len(valid_peers)
    peer_beta = valid_model.peer_beta if valid_model else None
    peer_band = valid_model.peer_residual_band_pct if valid_model else None
    return RelativeAssessment(
        benchmark=benchmark,
        benchmark_symbol=snapshot.benchmark_symbol.upper(),
        peers=_relative_outcome(
            snapshot.change_pct,
            peer_average,
            peer_band or neutral_band_pct,
            beta=peer_beta,
        ),
        peer_symbols=tuple(symbol for symbol, _ in valid_peers),
        peer_average_change_pct=peer_average,
        benchmark_normalized=benchmark_beta is not None,
        peers_normalized=peer_beta is not None,
        model_samples=min(
            valid_model.benchmark_samples,
            valid_model.peer_samples,
        )
        if valid_model
        else 0,
    )


def assess_market_situation(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
) -> SituationAssessment:
    outcomes = tuple(
        outcome
        for outcome in (relative.benchmark, relative.peers)
        if outcome is not RelativeOutcome.UNAVAILABLE
    )
    if len(outcomes) < 2:
        return SituationAssessment(
            verdict=SituationVerdict.UNAVAILABLE,
            confidence="low",
            sensitivity_adjusted=False,
        )
    adjusted = relative.benchmark_normalized and relative.peers_normalized
    confidence = "high" if adjusted else "medium"

    if snapshot.direction is Direction.UP:
        if all(outcome is RelativeOutcome.OUTPERFORM for outcome in outcomes):
            verdict = SituationVerdict.COMPANY_STRENGTH
        elif all(outcome is not RelativeOutcome.OUTPERFORM for outcome in outcomes):
            verdict = SituationVerdict.BROADLY_EXPLAINED
        else:
            verdict = SituationVerdict.MIXED
    elif snapshot.direction is Direction.DOWN:
        if all(outcome is RelativeOutcome.UNDERPERFORM for outcome in outcomes):
            verdict = SituationVerdict.COMPANY_WEAKNESS
        elif all(outcome is not RelativeOutcome.UNDERPERFORM for outcome in outcomes):
            verdict = SituationVerdict.BROADLY_EXPLAINED
        else:
            verdict = SituationVerdict.MIXED
    else:
        verdict = SituationVerdict.BROADLY_EXPLAINED
    return SituationAssessment(
        verdict=verdict,
        confidence=confidence,
        sensitivity_adjusted=adjusted,
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
    *,
    beta: float | None = None,
) -> RelativeOutcome:
    if comparison_change_pct is None:
        return RelativeOutcome.UNAVAILABLE
    expected_change = (
        comparison_change_pct
        if beta is None
        else beta * comparison_change_pct
    )
    difference = actual_change_pct - expected_change
    if difference > neutral_band_pct:
        return RelativeOutcome.OUTPERFORM
    if difference < -neutral_band_pct:
        return RelativeOutcome.UNDERPERFORM
    return RelativeOutcome.INLINE


def _matching_sensitivity(
    snapshot: MarketSnapshot,
    sensitivity: MarketSensitivity | None,
) -> MarketSensitivity | None:
    if sensitivity is None:
        return None
    if sensitivity.ticker != snapshot.ticker.upper():
        return None
    if sensitivity.benchmark_symbol != snapshot.benchmark_symbol.upper():
        return None
    if not set(sensitivity.peer_symbols).issuperset(
        symbol.upper() for symbol in snapshot.peer_changes
    ):
        return None
    return sensitivity
