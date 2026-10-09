from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from math import floor

from .levels import PriceLevels
from .models import (
    Direction,
    LevelEventSignal,
    MarketFrame,
    MarketSensitivity,
    MarketSnapshot,
    PriceBandSignal,
    PriceBandState,
    RapidMoveSignal,
    RelativeOutcome,
    RelativeStrength,
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
    # Model flags apply to the separate expected outcomes, not displayed returns.
    benchmark_normalized: bool = False
    peers_normalized: bool = False
    model_samples: int = 0
    benchmark_strength: RelativeStrength | None = None
    peer_strength: RelativeStrength | None = None
    benchmark_expected: RelativeOutcome | None = None
    peers_expected: RelativeOutcome | None = None


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


def level_break_buffer(levels: "PriceLevels", close_price: float) -> float:
    """Confirmation margin so a close barely past a level does not alert."""
    return max((levels.atr14 or 0.0) * 0.1, close_price * 0.0015)


def detect_level_events(
    levels: "PriceLevels",
    frame: MarketFrame,
    *,
    break_already,
) -> tuple[LevelEventSignal, ...]:
    """Closes through the level map, confirmed on the 5-minute close.

    Supports emit a downward break and, only after a confirmed break, an
    upward reclaim; resistances emit an upward break; moving averages emit
    a cross only when the previous session closed on the other side; the
    52-week extremes emit new-low/new-high events. Daily dedup happens via
    the event key, so a candidate may be produced more than once safely.
    """
    close = frame.close_price
    snapshot = frame.snapshot
    buffer = level_break_buffer(levels, close)
    prefix = f"{snapshot.ticker.upper()}:{snapshot.trading_date.isoformat()}:level"

    def signal(kind: str, direction: Direction, price: float, touches: int, key: str):
        return LevelEventSignal(
            event_key=key,
            ticker=snapshot.ticker.upper(),
            trading_date=snapshot.trading_date,
            kind=kind,
            direction=direction,
            level_price=price,
            touches=touches,
            close_price=close,
            observed_at=snapshot.observed_at,
            session=snapshot.session,
        )

    events: list[LevelEventSignal] = []
    for support in levels.supports:
        down_key = f"{prefix}:support:{support.price:.2f}:down"
        if close < support.price - buffer:
            events.append(
                signal("support", Direction.DOWN, support.price, support.touches, down_key)
            )
        elif close > support.price + buffer and break_already(down_key):
            events.append(
                signal(
                    "support",
                    Direction.UP,
                    support.price,
                    support.touches,
                    f"{prefix}:support:{support.price:.2f}:up",
                )
            )
    for resistance in levels.resistances:
        if close > resistance.price + buffer:
            events.append(
                signal(
                    "resistance",
                    Direction.UP,
                    resistance.price,
                    resistance.touches,
                    f"{prefix}:resistance:{resistance.price:.2f}:up",
                )
            )
    previous_close = levels.last_close
    if previous_close is not None:
        for kind, value in (
            ("sma20", levels.sma20),
            ("sma50", levels.sma50),
            ("sma200", levels.sma200),
        ):
            if value is None:
                continue
            if previous_close < value and close > value + buffer:
                events.append(
                    signal(kind, Direction.UP, value, 0, f"{prefix}:{kind}:up")
                )
            elif previous_close > value and close < value - buffer:
                events.append(
                    signal(kind, Direction.DOWN, value, 0, f"{prefix}:{kind}:down")
                )
    if levels.low_52w is not None and close < levels.low_52w:
        events.append(
            signal("52w-low", Direction.DOWN, levels.low_52w, 0, f"{prefix}:52w-low:down")
        )
    if levels.high_52w is not None and close > levels.high_52w:
        events.append(
            signal("52w-high", Direction.UP, levels.high_52w, 0, f"{prefix}:52w-high:up")
        )
    return tuple(events)


class RapidMovePolicy:
    """Velocity alarm: a sharp move on burst volume inside 15 minutes.

    Fires when the 15-minute price change and the 15-minute volume (vs the
    session's typical bar) both spike, then stays quiet per direction for
    the cooldown so a fast market does not spam the channel.
    """

    def __init__(
        self,
        *,
        move_threshold_pct: float = 1.5,
        volume_ratio_threshold: float = 2.5,
        cooldown: timedelta = timedelta(minutes=60),
    ) -> None:
        if move_threshold_pct <= 0 or volume_ratio_threshold <= 0:
            raise ValueError("rapid-move thresholds must be positive")
        self.move_threshold_pct = move_threshold_pct
        self.volume_ratio_threshold = volume_ratio_threshold
        self.cooldown = cooldown

    def evaluate(
        self,
        frame: MarketFrame,
        state: PriceBandState,
    ) -> tuple[RapidMoveSignal | None, PriceBandState]:
        change = frame.change_15m_pct
        ratio = frame.volume_15m_ratio
        if change is None or ratio is None:
            return None, state
        if abs(change) < self.move_threshold_pct or ratio < self.volume_ratio_threshold:
            return None, state
        direction = Direction.UP if change > 0 else Direction.DOWN
        observed_at = frame.snapshot.observed_at
        last_at = (
            state.rapid_up_last_at
            if direction is Direction.UP
            else state.rapid_down_last_at
        )
        if last_at is not None and observed_at - last_at < self.cooldown:
            return None, state
        if direction is Direction.UP:
            next_state = replace(state, rapid_up_last_at=observed_at)
        else:
            next_state = replace(state, rapid_down_last_at=observed_at)
        direction_token = "up" if direction is Direction.UP else "down"
        event_key = (
            f"{frame.snapshot.ticker.upper()}:{frame.snapshot.trading_date.isoformat()}:"
            f"rapid:{direction_token}:{observed_at.strftime('%H%M')}"
        )
        signal = RapidMoveSignal(
            event_key=event_key,
            ticker=frame.snapshot.ticker.upper(),
            trading_date=frame.snapshot.trading_date,
            direction=direction,
            change_15m_pct=change,
            volume_15m_ratio=ratio,
            observed_at=observed_at,
            session=frame.snapshot.session,
        )
        return signal, next_state


class PriceBandPolicy:
    def __init__(self, start_level: int = 3, step: int = 1) -> None:
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
    neutral_band_pct: float = 0.3,
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
        neutral_band_pct,
    )
    benchmark_strength = _relative_strength(
        snapshot.change_pct,
        snapshot.benchmark_change_pct,
        benchmark,
    )
    benchmark_expected = (
        _relative_outcome(
            snapshot.change_pct,
            snapshot.benchmark_change_pct,
            benchmark_band or neutral_band_pct,
            beta=benchmark_beta,
        )
        if benchmark_beta is not None
        else None
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
            benchmark_strength=benchmark_strength,
            benchmark_expected=benchmark_expected,
        )

    peer_average = sum(change for _, change in valid_peers) / len(valid_peers)
    peer_beta = valid_model.peer_beta if valid_model else None
    peer_band = valid_model.peer_residual_band_pct if valid_model else None
    peers = _relative_outcome(snapshot.change_pct, peer_average, neutral_band_pct)
    return RelativeAssessment(
        benchmark=benchmark,
        benchmark_symbol=snapshot.benchmark_symbol.upper(),
        peers=peers,
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
        benchmark_strength=benchmark_strength,
        peer_strength=_relative_strength(snapshot.change_pct, peer_average, peers),
        benchmark_expected=benchmark_expected,
        peers_expected=(
            _relative_outcome(
                snapshot.change_pct,
                peer_average,
                peer_band or neutral_band_pct,
                beta=peer_beta,
            )
            if peer_beta is not None
            else None
        ),
    )


def assess_market_situation(
    snapshot: MarketSnapshot,
    relative: RelativeAssessment,
) -> SituationAssessment:
    outcomes = tuple(
        outcome
        for outcome in (
            relative.benchmark_expected or relative.benchmark,
            relative.peers_expected or relative.peers,
        )
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
    # Subtracting returns can put an exact boundary just above it in binary floats.
    difference = round(actual_change_pct - expected_change, 10)
    if difference > neutral_band_pct:
        return RelativeOutcome.OUTPERFORM
    if difference < -neutral_band_pct:
        return RelativeOutcome.UNDERPERFORM
    return RelativeOutcome.INLINE


def _relative_strength(
    actual_change_pct: float,
    comparison_change_pct: float | None,
    outcome: RelativeOutcome,
) -> RelativeStrength | None:
    if comparison_change_pct is None or outcome in {
        RelativeOutcome.INLINE,
        RelativeOutcome.UNAVAILABLE,
    }:
        return None
    difference = round(abs(actual_change_pct - comparison_change_pct), 10)
    if difference <= 0.8:
        return RelativeStrength.SLIGHT
    if difference <= 2.0:
        return RelativeStrength.SIGNIFICANT
    return RelativeStrength.STRONG


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
