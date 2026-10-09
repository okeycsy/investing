from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class DailyBar:
    trading_date: date
    close: float
    high: float | None = None
    low: float | None = None


@dataclass(frozen=True)
class ClusteredLevel:
    price: float
    touches: int


@dataclass(frozen=True)
class PriceLevels:
    ticker: str
    trading_date: date
    computed_at: datetime
    sma20: float | None = None
    sma50: float | None = None
    sma200: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    atr14: float | None = None
    supports: tuple[ClusteredLevel, ...] = field(default_factory=tuple)
    resistances: tuple[ClusteredLevel, ...] = field(default_factory=tuple)
    last_close: float | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "trading_date": self.trading_date.isoformat(),
            "computed_at": self.computed_at.isoformat(),
            "sma20": self.sma20,
            "sma50": self.sma50,
            "sma200": self.sma200,
            "high_52w": self.high_52w,
            "low_52w": self.low_52w,
            "atr14": self.atr14,
            "supports": [
                {"price": level.price, "touches": level.touches}
                for level in self.supports
            ],
            "resistances": [
                {"price": level.price, "touches": level.touches}
                for level in self.resistances
            ],
            "last_close": self.last_close,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "PriceLevels":
        return cls(
            ticker=str(payload["ticker"]),
            trading_date=date.fromisoformat(str(payload["trading_date"])),
            computed_at=datetime.fromisoformat(str(payload["computed_at"])),
            sma20=payload.get("sma20"),
            sma50=payload.get("sma50"),
            sma200=payload.get("sma200"),
            high_52w=payload.get("high_52w"),
            low_52w=payload.get("low_52w"),
            atr14=payload.get("atr14"),
            supports=tuple(
                ClusteredLevel(item["price"], item["touches"])
                for item in payload.get("supports") or ()
            ),
            resistances=tuple(
                ClusteredLevel(item["price"], item["touches"])
                for item in payload.get("resistances") or ()
            ),
            last_close=payload.get("last_close"),
        )


def compute_price_levels(
    ticker: str,
    daily_bars: Sequence[DailyBar],
    *,
    trading_date: date,
    computed_at: datetime,
    cluster_window: int = 60,
) -> PriceLevels:
    """Compute moving averages, 52-week range, ATR and touch-tested levels.

    daily_bars must be completed sessions only (exclude the in-progress day),
    ordered oldest to newest.
    """
    history = [bar for bar in daily_bars if bar.trading_date < trading_date]
    closes = [bar.close for bar in history]
    if not closes:
        return PriceLevels(
            ticker=ticker.upper(),
            trading_date=trading_date,
            computed_at=computed_at,
        )
    last_close = closes[-1]
    atr = _wilder_atr(history)
    recent = history[-cluster_window:]
    tolerance = max((atr or 0.0) * 0.2, last_close * 0.0025)
    supports = _clusters(
        [bar.low if bar.low is not None else bar.close for bar in recent],
        tolerance,
        keep=lambda price: price <= last_close,
        prefer_near=last_close,
    )
    resistances = _clusters(
        [bar.high if bar.high is not None else bar.close for bar in recent],
        tolerance,
        keep=lambda price: price >= last_close,
        prefer_near=last_close,
    )
    highs = [bar.high if bar.high is not None else bar.close for bar in history]
    lows = [bar.low if bar.low is not None else bar.close for bar in history]
    return PriceLevels(
        ticker=ticker.upper(),
        trading_date=trading_date,
        computed_at=computed_at,
        sma20=_sma(closes, 20),
        sma50=_sma(closes, 50),
        sma200=_sma(closes, 200),
        high_52w=max(highs[-252:]) if highs else None,
        low_52w=min(lows[-252:]) if lows else None,
        atr14=atr,
        supports=supports,
        resistances=resistances,
        last_close=last_close,
    )


def _sma(closes: Sequence[float], window: int) -> float | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:]) / window


def _wilder_atr(history: Sequence[DailyBar], window: int = 14) -> float | None:
    if len(history) < window + 1:
        return None
    true_ranges = []
    for previous, current in zip(history, history[1:]):
        high = current.high if current.high is not None else current.close
        low = current.low if current.low is not None else current.close
        true_ranges.append(
            max(
                high - low,
                abs(high - previous.close),
                abs(low - previous.close),
            )
        )
    atr = sum(true_ranges[:window]) / window
    for value in true_ranges[window:]:
        atr = (atr * (window - 1) + value) / window
    return atr


def _clusters(
    prices: Sequence[float],
    tolerance: float,
    *,
    keep,
    prefer_near: float,
    limit: int = 2,
    minimum_touches: int = 2,
) -> tuple[ClusteredLevel, ...]:
    """Pick the most-touched price levels without chain-merging a trend.

    Every observed price is a candidate center scored by how many prices sit
    within the tolerance band around it; the best non-overlapping bands win,
    nearest to prefer_near on equal touches.
    """
    candidates = []
    for center in prices:
        members = [price for price in prices if abs(price - center) <= tolerance]
        if len(members) < minimum_touches:
            continue
        mean = sum(members) / len(members)
        if not keep(mean):
            continue
        candidates.append(ClusteredLevel(mean, len(members)))
    candidates.sort(
        key=lambda level: (-level.touches, abs(level.price - prefer_near))
    )
    selected: list[ClusteredLevel] = []
    for level in candidates:
        if any(abs(level.price - chosen.price) <= tolerance * 2 for chosen in selected):
            continue
        selected.append(level)
        if len(selected) == limit:
            break
    selected.sort(key=lambda level: abs(level.price - prefer_near))
    return tuple(selected)
