from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PriceData:
    current: float = 0.0
    prev_close: float = 0.0
    change_pct: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: int = 0
    vol_avg_5d: int = 0
    market_state: str = ""
    timestamp: str = ""


@dataclass
class TechnicalSignals:
    rsi_14: float = 50.0
    macd_line: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    rsi_alert: str = ""
    macd_alert: str = ""


@dataclass
class OptionsData:
    pcr: float = 0.0
    total_puts: int = 0
    total_calls: int = 0
    pcr_signal: str = ""


@dataclass
class ShortInterestData:
    short_volume: int = 0
    total_volume: int = 0
    short_pct: float = 0.0
    date: str = ""
    signal: str = ""


@dataclass
class InsiderTrade:
    filer: str = ""
    title: str = ""
    trade_type: str = ""
    txn_code: str = ""
    shares: int = 0
    price: float = 0.0
    total_value: float = 0.0
    date: str = ""
    url: str = ""


@dataclass
class Filing13F:
    institution: str = ""
    shares: int = 0
    value_usd: float = 0.0
    change_type: str = ""
    filing_date: str = ""
    url: str = ""


@dataclass
class VolumeProfile:
    poc_price: float = 0.0
    current_price: float = 0.0
    poc_signal: str = ""
    vol_30m: int = 0
    vol_avg_30m: int = 0
    vol_ratio: float = 0.0
    whale_detected: bool = False


@dataclass
class SafetyMargin:
    bb_lower: float = 0.0
    bb_upper: float = 0.0
    sma20: float = 0.0
    current_price: float = 0.0
    pct_from_lower: float = 0.0
    bb_signal: str = ""
    mom_30m_prev: float = 0.0
    mom_30m_curr: float = 0.0
    momentum_signal: str = ""
    beta_expected_pct: float = 0.0
    beta_excess_pct: float = 0.0
    peer_coin_pct: float = 0.0
    peer_mstr_pct: float = 0.0
    peer_changes: dict = field(default_factory=dict)
    divergence_warning: bool = False
    dca_attraction: int = 0


@dataclass
class DCAScoreItem:
    label: str = ""
    score: int = 0
    max_pts: int = 0
    desc: str = ""


@dataclass
class DCALayerScore:
    name: str = ""
    layer_id: str = ""
    pts: int = 0
    max_pts: int = 0
    items: list = field(default_factory=list)


@dataclass
class DCATechnicalScore:
    layers: list = field(default_factory=list)
    total: int = 0
    grade: str = ""
    grade_emoji: str = ""
