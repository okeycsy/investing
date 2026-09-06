from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from math import sqrt

from investing_monitor.domain.models import MarketSensitivity


MINIMUM_SENSITIVITY_SAMPLES = 40
MINIMUM_RESIDUAL_BAND_PCT = 0.75
MAXIMUM_RESIDUAL_BAND_PCT = 3.0


@dataclass(frozen=True)
class SensitivityFit:
    beta: float
    residual_band_pct: float
    samples: int


@dataclass(frozen=True)
class MarketContextDelta:
    situation: tuple[str, str] | None = None
    benchmark: tuple[str, str] | None = None
    peers: tuple[str, str] | None = None
    volume: tuple[str, str] | None = None
    new_catalyst_count: int = 0

    @property
    def changed(self) -> bool:
        return bool(
            self.situation
            or self.benchmark
            or self.peers
            or self.volume
            or self.new_catalyst_count
        )


def compare_market_context(
    previous: Mapping[str, object] | None,
    current: Mapping[str, object],
) -> MarketContextDelta | None:
    if not previous:
        return None

    def transition(key: str) -> tuple[str, str] | None:
        before = str(previous.get(key) or "")
        after = str(current.get(key) or "")
        if not before or not after or before == after:
            return None
        return before, after

    previous_ids = {
        str(value) for value in previous.get("catalyst_ids", ()) if str(value)
    }
    current_ids = {
        str(value) for value in current.get("catalyst_ids", ()) if str(value)
    }
    return MarketContextDelta(
        situation=transition("situation"),
        benchmark=transition("benchmark_outcome"),
        peers=transition("peer_outcome"),
        volume=transition("volume_status"),
        new_catalyst_count=len(current_ids - previous_ids),
    )


def build_market_sensitivity(
    *,
    ticker: str,
    benchmark_symbol: str,
    peer_symbols: Sequence[str],
    daily_closes: Mapping[str, Mapping[date, float]],
    calculated_at: datetime,
    minimum_samples: int = MINIMUM_SENSITIVITY_SAMPLES,
) -> MarketSensitivity:
    target_returns = _daily_returns(daily_closes.get(ticker.upper(), {}))
    benchmark_returns = _daily_returns(
        daily_closes.get(benchmark_symbol.upper(), {})
    )
    normalized_peers = tuple(symbol.upper() for symbol in peer_symbols)
    peer_returns = {
        symbol: _daily_returns(daily_closes.get(symbol, {}))
        for symbol in normalized_peers
    }
    peer_average = _peer_average_returns(peer_returns)
    benchmark_fit = _fit_sensitivity(
        target_returns,
        benchmark_returns,
        minimum_samples=minimum_samples,
    )
    peer_fit = _fit_sensitivity(
        target_returns,
        peer_average,
        minimum_samples=minimum_samples,
    )
    return MarketSensitivity(
        ticker=ticker,
        benchmark_symbol=benchmark_symbol,
        peer_symbols=normalized_peers,
        calculated_at=calculated_at,
        benchmark_beta=benchmark_fit.beta if benchmark_fit else None,
        benchmark_residual_band_pct=(
            benchmark_fit.residual_band_pct if benchmark_fit else None
        ),
        benchmark_samples=benchmark_fit.samples if benchmark_fit else 0,
        peer_beta=peer_fit.beta if peer_fit else None,
        peer_residual_band_pct=peer_fit.residual_band_pct if peer_fit else None,
        peer_samples=peer_fit.samples if peer_fit else 0,
    )


def _daily_returns(closes: Mapping[date, float]) -> dict[date, float]:
    returns: dict[date, float] = {}
    previous: float | None = None
    for trading_date, close in sorted(closes.items()):
        value = float(close)
        if value <= 0:
            continue
        if previous is not None:
            returns[trading_date] = (value / previous - 1.0) * 100.0
        previous = value
    return returns


def _peer_average_returns(
    peer_returns: Mapping[str, Mapping[date, float]],
    *,
    minimum_peers: int = 2,
) -> dict[date, float]:
    dates = set().union(*(values.keys() for values in peer_returns.values()))
    averages = {}
    for trading_date in dates:
        values = [
            returns[trading_date]
            for returns in peer_returns.values()
            if trading_date in returns
        ]
        if len(values) >= minimum_peers:
            averages[trading_date] = sum(values) / len(values)
    return averages


def _fit_sensitivity(
    target: Mapping[date, float],
    comparison: Mapping[date, float],
    *,
    minimum_samples: int,
) -> SensitivityFit | None:
    dates = sorted(target.keys() & comparison.keys())
    if len(dates) < minimum_samples:
        return None
    x = [float(comparison[value]) for value in dates]
    y = [float(target[value]) for value in dates]
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    variance_x = sum((value - mean_x) ** 2 for value in x)
    if variance_x <= 1e-9:
        return None
    beta = sum(
        (left - mean_x) * (right - mean_y)
        for left, right in zip(x, y, strict=True)
    ) / variance_x
    beta = max(-1.0, min(4.0, beta))
    alpha = mean_y - beta * mean_x
    residuals = [
        target_value - (alpha + beta * comparison_value)
        for comparison_value, target_value in zip(x, y, strict=True)
    ]
    residual_mean = sum(residuals) / len(residuals)
    variance = sum((value - residual_mean) ** 2 for value in residuals)
    residual_std = sqrt(variance / max(1, len(residuals) - 1))
    residual_band = max(
        MINIMUM_RESIDUAL_BAND_PCT,
        min(MAXIMUM_RESIDUAL_BAND_PCT, residual_std),
    )
    return SensitivityFit(
        beta=beta,
        residual_band_pct=residual_band,
        samples=len(dates),
    )
