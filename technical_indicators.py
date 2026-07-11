from __future__ import annotations

from typing import Optional

from monitor_models import TechnicalSignals


def calculate_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [delta if delta > 0 else 0 for delta in deltas]
    losses = [-delta if delta < 0 else 0 for delta in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for idx in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[idx]) / period
        avg_loss = (avg_loss * (period - 1) + losses[idx]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def calculate_macd(closes: list) -> tuple:
    if len(closes) < 35:
        return 0.0, 0.0, 0.0

    def ema(data, period):
        k = 2 / (period + 1)
        result = [data[0]]
        for idx in range(1, len(data)):
            result.append(data[idx] * k + result[-1] * (1 - k))
        return result

    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    macd = [e12[idx] - e26[idx] for idx in range(len(closes))]
    sig = ema(macd, 9)
    return round(macd[-1], 4), round(sig[-1], 4), round(macd[-1] - sig[-1], 4)


def get_technical_signals(closes: list) -> TechnicalSignals:
    rsi = calculate_rsi(closes)
    macd_line, macd_sig, macd_hist = calculate_macd(closes)
    signals = TechnicalSignals(
        rsi_14=rsi,
        macd_line=macd_line,
        macd_signal=macd_sig,
        macd_histogram=macd_hist,
    )
    if rsi <= 30:
        signals.rsi_alert = "oversold"
    elif rsi >= 70:
        signals.rsi_alert = "overbought"
    if len(closes) >= 36:
        prev_macd, prev_signal, _ = calculate_macd(closes[:-1])
        if prev_macd < prev_signal and macd_line > macd_sig:
            signals.macd_alert = "bullish_cross"
        elif prev_macd > prev_signal and macd_line < macd_sig:
            signals.macd_alert = "bearish_cross"
    return signals


def calc_ema_series(data: list, period: int) -> list:
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for value in data[period:]:
        ema.append(value * k + ema[-1] * (1 - k))
    return ema


def calc_obv(closes: list, volumes: list) -> list:
    if len(closes) < 2 or len(volumes) < 2:
        return []
    obv = [0]
    for idx in range(1, min(len(closes), len(volumes))):
        if closes[idx] > closes[idx - 1]:
            obv.append(obv[-1] + volumes[idx])
        elif closes[idx] < closes[idx - 1]:
            obv.append(obv[-1] - volumes[idx])
        else:
            obv.append(obv[-1])
    return obv


def calc_mfi(highs: list, lows: list, closes: list, volumes: list, period: int = 14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 1:
        return None
    typical = [(highs[idx] + lows[idx] + closes[idx]) / 3 for idx in range(n)]
    pos_mf = neg_mf = 0.0
    for idx in range(n - period, n):
        mf = typical[idx] * volumes[idx]
        if typical[idx] > typical[idx - 1]:
            pos_mf += mf
        else:
            neg_mf += mf
    if neg_mf == 0:
        return 100.0
    return round(100 - 100 / (1 + pos_mf / neg_mf), 2)


def calc_stochastic(
    highs: list,
    lows: list,
    closes: list,
    period: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
) -> tuple:
    n = min(len(highs), len(lows), len(closes))
    if n < period + smooth_k + smooth_d:
        return None, None
    raw_k = []
    for idx in range(period - 1, n):
        hh = max(highs[idx - period + 1: idx + 1])
        ll = min(lows[idx - period + 1: idx + 1])
        raw_k.append(((closes[idx] - ll) / (hh - ll) * 100) if hh != ll else 50.0)
    if len(raw_k) < smooth_k:
        return None, None
    k_series = [
        sum(raw_k[idx - smooth_k + 1: idx + 1]) / smooth_k
        for idx in range(smooth_k - 1, len(raw_k))
    ]
    if len(k_series) < smooth_d:
        return None, None
    d_val = sum(k_series[-smooth_d:]) / smooth_d
    return round(k_series[-1], 2), round(d_val, 2)


def calc_atr(highs: list, lows: list, closes: list, period: int = 14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    tr_list = [
        max(
            highs[idx] - lows[idx],
            abs(highs[idx] - closes[idx - 1]),
            abs(lows[idx] - closes[idx - 1]),
        )
        for idx in range(1, n)
    ]
    if len(tr_list) < period:
        return None
    return round(sum(tr_list[-period:]) / period, 4)


def detect_rsi_bullish_divergence(closes: list, lookback: int = 20) -> bool:
    if len(closes) < lookback + 14:
        return False
    window = closes[-(lookback + 14):]
    rsi_series = [calculate_rsi(window[:idx + 1]) for idx in range(14, len(window))]
    if len(rsi_series) < lookback:
        return False
    price_w = closes[-lookback:]
    rsi_w = rsi_series[-lookback:]
    mid = len(price_w) // 2
    p1_low = min(price_w[:mid])
    p2_low = min(price_w[mid:])
    r1_low = min(rsi_w[:mid])
    r2_low = min(rsi_w[mid:])
    return p2_low < p1_low and r2_low > r1_low


def calc_cmf(highs: list, lows: list, closes: list, volumes: list, period: int = 21) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period:
        return None
    mfv_sum = vol_sum = 0.0
    for idx in range(n - period, n):
        hl = highs[idx] - lows[idx]
        if hl == 0:
            continue
        mfm = ((closes[idx] - lows[idx]) - (highs[idx] - closes[idx])) / hl
        mfv_sum += mfm * volumes[idx]
        vol_sum += volumes[idx]
    return round(mfv_sum / vol_sum, 4) if vol_sum else 0.0


def calc_daily_hvn(highs: list, lows: list, closes: list, volumes: list, lookback: int = 60) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes), len(volumes))
    lookback = min(lookback, n)
    if lookback < 20:
        return None
    current = closes[-1]
    if current <= 0:
        return None
    bucket_size = 0.005
    vol_by_bucket: dict = {}
    for idx in range(n - lookback, n):
        typical_price = (highs[idx] + lows[idx] + closes[idx]) / 3
        pct = typical_price / current - 1
        bucket = round(pct / bucket_size) * bucket_size
        vol_by_bucket[bucket] = vol_by_bucket.get(bucket, 0) + volumes[idx]
    if not vol_by_bucket:
        return None
    hvn_bucket = max(vol_by_bucket, key=vol_by_bucket.get)
    return round(hvn_bucket * 100, 2)
