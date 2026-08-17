#!/usr/bin/env python3
"""
Configurable Ticker Advanced Monitor v3.2
=========================================
v3.2 fixes:
  1. Form 4 404 → 신고자 CIK를 accession 번호에서 추출
  2. FINRA short interest → 당일 제외, float 파싱 수정
  3. Yahoo Options 401 → graceful skip
  4. run_close() 에 등락률 로깅 추가
  5. 4% 미만일 때도 종가 방향 이모지 표시
"""

import os
import sys
import json
import hashlib
import math
import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import requests

from monitor_config import load_monitor_config, resolve_runtime_file

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
CONFIG = load_monitor_config()
TICKER = CONFIG.ticker
DISPLAY_TICKER = CONFIG.display_ticker
COMPANY_NAME = CONFIG.company_name
CIK = CONFIG.cik.strip()
CIK_PADDED = CIK.zfill(10) if CIK else ""  # 10자리 (선행 0 포함)
CIK_SHORT = CIK.lstrip("0")                # 선행 0 제거
PEER_TICKERS = CONFIG.peer_tickers
COMPANY_ALIASES = CONFIG.company_aliases
NEWS_TERMS = CONFIG.news_terms
PRIORITY_KEYWORDS = CONFIG.priority_keywords
RISK_KEYWORDS = CONFIG.risk_keywords
CORE_KPIS = CONFIG.core_kpis
PROFILE_CONTEXT = CONFIG.profile_context

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STATE_FILE = resolve_runtime_file(CONFIG, "state.json", "MONITOR_STATE_FILE")
WEEKLY_STATE_FILE = resolve_runtime_file(CONFIG, "weekly_state.json", "MONITOR_WEEKLY_STATE_FILE")

YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
YAHOO_QUOTE_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
]
YAHOO_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

SEC_HEADERS = CONFIG.sec_headers
SEC_LEGACY_HEADERS = CONFIG.sec_legacy_headers
FINRA_SHORT_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
NY_TZ = ZoneInfo("America/New_York")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hood_monitor")
SOURCE_HEALTH: dict[str, str] = {}
VOLUME_LOOKBACK_DAYS = 20
VOLUME_EXPLOSION_RATIO = 1.5


def _set_source_health(source: str, status: str):
    SOURCE_HEALTH[source] = status


class SyntheticYahooResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self._payload


# ─────────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────────
@dataclass
class PriceData:
    current: float = 0.0
    prev_close: float = 0.0
    change_pct: float = 0.0
    high: float = 0.0
    low: float = 0.0
    volume: int = 0
    vol_avg_5d: int = 0
    vol_avg_20d: int = 0
    market_state: str = ""    # "REGULAR" | "PRE" | "POST" | "CLOSED"
    timestamp: str = ""
    market_date: str = ""


@dataclass
class VolumeActivity:
    current_volume: int = 0
    average_volume: int = 0
    ratio: float = 0.0
    window: int = VOLUME_LOOKBACK_DAYS
    exploded: bool = False


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
    trade_type: str = ""   # "Purchase" | "Sale" | "Award"
    txn_code: str = ""     # 원본 SEC transaction code (P/S/A/D 등)
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
class CompanyFiling:
    form: str = ""
    filing_date: str = ""
    report_date: str = ""
    description: str = ""
    accession: str = ""
    url: str = ""
    hash: str = ""


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


# ─────────────────────────────────────────────
# 상태 관리
# ─────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "last_news_hashes": [],
        "last_insider_hashes": [],
        "last_13f_hashes": [],
        "last_company_filing_hashes": [],
        "last_13f_positions": {},
        "price_history": [],
        "price_alert_max_pct": 0,
        "price_alert_direction": "",
        "price_alert_date": "",
        "volume_alert_date": "",
        "pending_morning_alert": None,
        # ── DCA 포트폴리오 ──
        "dca_shares": 0.0,        # 총 보유 수량
        "dca_avg_price": 0.0,     # 평균 매수가
        "dca_history": [],        # 매수 이력
    }


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def load_weekly_state() -> dict:
    if WEEKLY_STATE_FILE.exists():
        try:
            return json.loads(WEEKLY_STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "week_start": "",
        "alerts_fired": [],
        "insider_trades": [],
        "news_headlines": [],
        "company_filings": [],
        "thesis_impacts": [],
        "rsi_readings": [],
        "pcr_readings": [],
        "short_readings": [],
    }


def save_weekly_state(ws: dict):
    WEEKLY_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    WEEKLY_STATE_FILE.write_text(json.dumps(ws, indent=2, ensure_ascii=False))


def _merge_hashes(existing: list, new_hashes: list, limit: int = 60) -> list:
    return list(dict.fromkeys([*new_hashes, *existing]))[:limit]


def remember_analyzed_news(state: dict, news: list):
    hashes = analyzed_news_hashes(news)
    if hashes:
        state["last_news_hashes"] = _merge_hashes(
            state.get("last_news_hashes", []), hashes
        )


def remember_company_filings(state: dict, filings: list):
    hashes = [filing.hash for filing in filings if filing.hash]
    if hashes:
        state["last_company_filing_hashes"] = _merge_hashes(
            state.get("last_company_filing_hashes", []), hashes
        )


# ─────────────────────────────────────────────
# HTTP 유틸
# ─────────────────────────────────────────────
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_last_yahoo_call = 0.0


def _yahoo_throttle():
    global _last_yahoo_call
    elapsed = time.time() - _last_yahoo_call
    if elapsed < 1.5:
        time.sleep(1.5 - elapsed)
    _last_yahoo_call = time.time()


def safe_get(url, headers=None, params=None, timeout=15, retries=3):
    h = headers.copy() if headers else {}
    if "User-Agent" not in h:
        h["User-Agent"] = BROWSER_UA
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=h, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 503):
                wait = min(2 ** (attempt + 1), 16)
                log.warning(f"HTTP {resp.status_code} — retry in {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            log.warning(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            log.error(f"Request failed: {e}")
            if attempt < retries - 1:
                time.sleep(1)
    return None


def fetch_yahoo_chart(ticker: str, params: dict, timeout: int = 15):
    """Try both Yahoo chart hosts because query1/query2 can be rate-limited independently."""
    for template in YAHOO_QUOTE_URLS:
        resp = safe_get(template.format(ticker=ticker), params=params, timeout=timeout)
        if resp:
            return resp
    fallback = _fetch_yfinance_chart(ticker, params)
    if fallback:
        log.info(f"Yahoo chart fallback via yfinance: {ticker} {params}")
        return SyntheticYahooResponse(fallback)
    return None


def _fetch_yfinance_chart(ticker: str, params: dict) -> Optional[dict]:
    try:
        import yfinance as yf
    except Exception as e:
        log.warning(f"yfinance fallback unavailable: {e}")
        return None

    period = params.get("range", "10d")
    interval = params.get("interval", "1d")
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            prepost=bool(params.get("includePrePost")),
            threads=False,
        )
    except Exception as e:
        log.warning(f"yfinance fallback failed ({ticker}): {e}")
        return None

    if df is None or df.empty:
        return None
    if hasattr(df.columns, "get_level_values"):
        try:
            price_cols = {"Open", "High", "Low", "Close", "Volume", "Adj Close"}
            if price_cols & set(df.columns.get_level_values(0)):
                df.columns = df.columns.get_level_values(0)
            else:
                df.columns = df.columns.get_level_values(1)
        except Exception:
            pass

    def clean_number(value):
        try:
            if value is None or math.isnan(float(value)):
                return None
            return float(value)
        except Exception:
            return None

    timestamps = []
    for idx in df.index:
        try:
            timestamps.append(int(idx.timestamp()))
        except Exception:
            timestamps.append(int(datetime.combine(idx.date(), datetime.min.time(), UTC).timestamp()))

    quote = {}
    for col, key in [("Open", "open"), ("High", "high"), ("Low", "low"), ("Close", "close")]:
        quote[key] = [clean_number(v) for v in df[col].tolist()] if col in df else []
    quote["volume"] = [int(v) if clean_number(v) is not None else 0 for v in df["Volume"].tolist()] if "Volume" in df else []

    closes = [v for v in quote.get("close", []) if v is not None]
    highs = [v for v in quote.get("high", []) if v is not None]
    lows = [v for v in quote.get("low", []) if v is not None]
    last_close = closes[-1] if closes else 0.0

    return {
        "chart": {
            "result": [{
                "timestamp": timestamps,
                "meta": {
                    "currency": "USD",
                    "regularMarketPrice": last_close,
                    "regularMarketDayHigh": highs[-1] if highs else 0.0,
                    "regularMarketDayLow": lows[-1] if lows else 0.0,
                },
                "indicators": {"quote": [quote]},
            }],
            "error": None,
        }
    }


# ─────────────────────────────────────────────
# 1. 주가 데이터
# ─────────────────────────────────────────────
def fetch_price(realtime: bool = True) -> Optional[PriceData]:
    """
    시장 상태별 정확한 현재가/전일종가 계산.

    핵심 수정: REGULAR 중 Yahoo 일봉 API는 closes_daily[-1]에 오늘 인트라데이
    바를 포함하기도 함 → prev_close = closes_daily[-1]이면 current ≈ prev_close
    → 4%+ 급등도 -0.01%로 오인.

    수정: 일봉 timestamps를 파싱해 오늘 날짜(UTC) 바를 제거,
    가장 최근 확정 종가(전일)만 prev_close로 사용.
    """
    _yahoo_throttle()
    # ── 1. 일봉 (timestamps 포함하여 오늘 바 필터링) ──────────────
    resp_1d = fetch_yahoo_chart(TICKER, {"interval": "1d", "range": "3mo"})
    if not resp_1d:
        return None
    try:
        result_1d        = resp_1d.json()["chart"]["result"][0]
        ts_daily_raw     = result_1d.get("timestamp", [])
        q1d              = result_1d["indicators"]["quote"][0]
        closes_raw       = q1d.get("close", [])
        volumes_raw      = q1d.get("volume", [])
    except Exception as e:
        log.error(f"fetch_price 일봉 파싱 실패: {e}")
        return None

    # ── 2. 장 상태 판별 ──────────────────────────────────────────
    now_ny = datetime.now(NY_TZ)
    today_market = now_ny.date()
    hm  = now_ny.hour * 60 + now_ny.minute
    dow = now_ny.weekday()  # 0=Mon 6=Sun

    if dow >= 5:
        market_state = "CLOSED"
    elif hm < 4 * 60:
        market_state = "CLOSED"
    elif hm < 9 * 60 + 30:
        market_state = "PRE"
    elif hm < 16 * 60:
        market_state = "REGULAR"
    elif hm < 20 * 60:
        market_state = "POST"
    else:
        market_state = "CLOSED"

    # ── 3. 확정 일봉만 추출 (오늘 날짜 바 제거) ──────────────────
    # Yahoo는 장 중에도 오늘 인트라데이 값을 일봉 시리즈 마지막에 넣을 수 있음.
    # 이를 그대로 prev_close로 쓰면 current ≈ prev_close → 등락률 0%에 가까워짐.
    confirmed_closes  = []
    confirmed_volumes = []
    for i, ts in enumerate(ts_daily_raw):
        bar_date = datetime.fromtimestamp(ts, NY_TZ).date()
        if bar_date < today_market:
            c = closes_raw[i] if i < len(closes_raw) else None
            v = volumes_raw[i] if i < len(volumes_raw) else None
            if c is not None:
                confirmed_closes.append(float(c))
            if v is not None:
                confirmed_volumes.append(int(v))

    if len(confirmed_closes) < 2:
        log.warning(f"fetch_price: 확정 일봉 부족 ({len(confirmed_closes)}개) — 전체 일봉 fallback")
        # fallback: 오늘 바 포함하더라도 [-2]를 prev_close로
        all_closes = [float(c) for c in closes_raw if c is not None]
        if len(all_closes) < 2:
            return None
        confirmed_closes = all_closes
        if not confirmed_volumes:
            all_volumes = [int(v) for v in volumes_raw if v is not None]
            confirmed_volumes = all_volumes[:-1]

    # prev_close = 가장 최근 확정 종가 (= 전일 정규장 종가)
    prev_close = round(confirmed_closes[-1], 2)

    # ── 4. 현재가 ────────────────────────────────────────────────
    if realtime and market_state in ("PRE", "REGULAR", "POST"):
        resp_1m = fetch_yahoo_chart(TICKER, {"interval": "1m", "range": "2d", "includePrePost": "true"})
        if not resp_1m:
            return None
        try:
            result_1m  = resp_1m.json()["chart"]["result"][0]
            timestamps = result_1m.get("timestamp", [])
            closes_1m  = result_1m["indicators"]["quote"][0].get("close", [])
            volumes_1m = result_1m["indicators"]["quote"][0].get("volume", [])
            current    = None
            today_vol  = 0
            for i in range(len(timestamps) - 1, -1, -1):
                bar_date = datetime.fromtimestamp(timestamps[i], NY_TZ).date()
                if bar_date == today_market:
                    if current is None and i < len(closes_1m) and closes_1m[i] is not None:
                        current = round(float(closes_1m[i]), 2)
                    if i < len(volumes_1m) and volumes_1m[i]:
                        today_vol += int(volumes_1m[i])
                elif bar_date < today_market and current is not None:
                    break  # 오늘 바 모두 처리 완료
        except Exception as e:
            log.error(f"fetch_price 1분봉 파싱 실패: {e}")
            return None

        if current is None:
            log.warning("fetch_price: 오늘 1분봉 바 없음 — 전일 종가로 대체")
            current   = prev_close
            today_vol = 0
    else:
        # CLOSED/realtime=False: 확정 일봉 사용
        # 오늘 바가 이미 확정됐으면 confirmed_closes[-1]이 오늘 종가일 수 있으므로
        # realtime=False(run_close)는 [-1] vs [-2] 사용
        if not realtime:
            # run_close 전용: 오늘 확정 종가(방금 마감) vs 전일 종가
            valid_indices = [i for i, close in enumerate(closes_raw) if close is not None]
            if len(valid_indices) < 2:
                return None
            latest_idx = valid_indices[-1]
            previous_idx = valid_indices[-2]
            current = round(float(closes_raw[latest_idx]), 2)
            prev_close = round(float(closes_raw[previous_idx]), 2)
            today_vol = (
                int(volumes_raw[latest_idx])
                if latest_idx < len(volumes_raw) and volumes_raw[latest_idx]
                else 0
            )
        else:
            current = prev_close  # CLOSED + realtime=True: 변동 없음
            today_vol = int(confirmed_volumes[-1]) if confirmed_volumes else 0

    change_pct = round((current - prev_close) / prev_close * 100, 2) if prev_close else 0

    past_vols = [volume for volume in confirmed_volumes if volume > 0]
    vol_avg_5d = int(sum(past_vols[-5:]) / len(past_vols[-5:])) if past_vols else 0
    vol_avg_20d = (
        int(sum(past_vols[-VOLUME_LOOKBACK_DAYS:]) / VOLUME_LOOKBACK_DAYS)
        if len(past_vols) >= VOLUME_LOOKBACK_DAYS else 0
    )

    log.info(
        f"fetch_price: state={market_state} "
        f"current={current:.2f} prev={prev_close:.2f} chg={change_pct:+.2f}% "
        f"confirmed_bars={len(confirmed_closes)}"
    )

    try:
        meta = result_1d["meta"]
    except Exception:
        meta = {}

    return PriceData(
        current=current,
        prev_close=prev_close,
        change_pct=change_pct,
        high=round(meta.get("regularMarketDayHigh", 0), 2),
        low=round(meta.get("regularMarketDayLow", 0), 2),
        volume=today_vol,
        vol_avg_5d=vol_avg_5d,
        vol_avg_20d=vol_avg_20d,
        market_state=market_state,
        timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        market_date=(
            datetime.fromtimestamp(ts_daily_raw[-1], NY_TZ).strftime("%Y-%m-%d")
            if ts_daily_raw else ""
        ),
    )


def fetch_price_history(days: int = 60) -> list:
    _yahoo_throttle()
    resp = fetch_yahoo_chart(TICKER, {"interval": "1d", "range": f"{days}d"})
    if not resp:
        return []
    try:
        data = resp.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [round(c, 2) for c in closes if c is not None]
    except Exception as e:
        log.error(f"Price history error: {e}")
        return []


# ─────────────────────────────────────────────
# 1-1. 베타(β) 계산 + 기대수익률 이격도
# ─────────────────────────────────────────────
BETA_CACHE_FILE = resolve_runtime_file(CONFIG, "beta_cache.json", "MONITOR_BETA_CACHE_FILE")
BETA_BENCHMARK = CONFIG.benchmark


def _fetch_yearly_closes(ticker: str) -> list:
    """1년치 일간 종가 반환"""
    _yahoo_throttle()
    resp = fetch_yahoo_chart(ticker, {"interval": "1d", "range": "1y"})
    if not resp:
        return []
    try:
        closes = resp.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except Exception as e:
        log.error(f"_fetch_yearly_closes({ticker}): {e}")
        return []


def _calc_beta(stock_closes: list, market_closes: list) -> Optional[float]:
    """β = Cov(r_s, r_m) / Var(r_m)"""
    n = min(len(stock_closes), len(market_closes))
    if n < 30:
        return None
    # 수익률 계산 (일간 단순 수익률)
    stock_window = stock_closes[-n:]
    market_window = market_closes[-n:]
    rs = [(stock_window[i] - stock_window[i-1]) / stock_window[i-1]
          for i in range(1, n)
          if stock_window[i-1] != 0]
    rm = [(market_window[i] - market_window[i-1]) / market_window[i-1]
          for i in range(1, n)
          if market_window[i-1] != 0]

    # 길이 맞추기
    length = min(len(rs), len(rm))
    rs, rm = rs[-length:], rm[-length:]
    if length < 20:
        return None

    mean_rs = sum(rs) / length
    mean_rm = sum(rm) / length
    cov = sum((rs[i] - mean_rs) * (rm[i] - mean_rm) for i in range(length)) / length
    var_m = sum((rm[i] - mean_rm) ** 2 for i in range(length)) / length
    if var_m == 0:
        return None
    return round(cov / var_m, 3)


def get_beta() -> Optional[float]:
    """
    베타 반환. 오늘 이미 계산된 캐시가 있으면 그걸 사용,
    없거나 날짜 다르면 1년치 데이터 재계산 후 캐시 저장.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    # 캐시 확인
    if BETA_CACHE_FILE.exists():
        try:
            cache = json.loads(BETA_CACHE_FILE.read_text())
            if cache.get("date") == today and cache.get("beta") is not None:
                log.info(f"Beta 캐시 히트: β={cache['beta']} ({today})")
                return float(cache["beta"])
        except Exception:
            pass

    # 재계산
    log.info("Beta 재계산 시작 (1년치 데이터 fetch)...")
    stock_closes = _fetch_yearly_closes(TICKER)
    market_closes = _fetch_yearly_closes(BETA_BENCHMARK)

    if not stock_closes or not market_closes:
        log.warning("Beta 계산 실패: 데이터 부족")
        return None

    beta = _calc_beta(stock_closes, market_closes)
    if beta is None:
        log.warning("Beta 계산 실패: 수익률 데이터 부족")
        return None

    # 캐시 저장
    BETA_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    BETA_CACHE_FILE.write_text(json.dumps({"date": today, "beta": beta}, indent=2))
    log.info(f"Beta 계산 완료: β={beta} → 캐시 저장")
    return beta


def calc_beta_divergence(beta: float, market_pct: float, actual_pct: float) -> dict:
    """
    기대 수익률 vs 실제 수익률 이격도 + 피어 그룹 비교 통합.
    피어 데이터는 monitor_config.md의 peer_tickers를 기준으로 fetch.
    """
    expected = round(beta * market_pct, 2)
    divergence = round(actual_pct - expected, 2)

    # 피어 데이터 fetch (상대 강도 블록에 통합 표시)
    peer_changes = {
        peer: (_fetch_ticker_change(peer) or 0.0)
        for peer in PEER_TICKERS
    }

    # 피어 평균 대비 모니터링 종목 이격
    peer_values = list(peer_changes.values())
    peer_avg = round(sum(peer_values) / len(peer_values), 2) if peer_values else 0.0
    peer_diff = round(actual_pct - peer_avg, 2)

    return {
        "beta": beta,
        "qqq_pct": market_pct,
        "expected_pct": expected,
        "actual_pct": actual_pct,
        "divergence": divergence,
        "peer_changes": peer_changes,
        "peer_avg": peer_avg,
        "peer_diff": peer_diff,
    }


def _direction_label(change_pct: Optional[float]) -> str:
    if change_pct is None or abs(change_pct) < 0.01:
        return "보합"
    return "양전" if change_pct > 0 else "음전"


def _relative_label(actual_pct: Optional[float], benchmark_pct: Optional[float]) -> str:
    if actual_pct is None or benchmark_pct is None:
        return f"{BETA_BENCHMARK} 비교 불가"
    diff = actual_pct - benchmark_pct
    if diff > 0.15:
        return f"{BETA_BENCHMARK} 대비 아웃퍼폼"
    if diff < -0.15:
        return f"{BETA_BENCHMARK} 대비 언더퍼폼"
    return f"{BETA_BENCHMARK}와 동조"


def format_beta_block(bd: dict) -> list:
    """가격과 등락률 숫자 없이 방향과 벤치마크 상대 성과만 표시."""
    actual = bd.get("actual_pct")
    benchmark_pct = bd.get("qqq_pct")
    direction = _direction_label(actual)
    relative = _relative_label(actual, benchmark_pct)
    icon = "🟢" if "아웃퍼폼" in relative else "🔴" if "언더퍼폼" in relative else "⚪"
    return [
        _sec(
            f"*📐 방향 / {BETA_BENCHMARK} 상대 성과*\n"
            f"{DISPLAY_TICKER}: *{direction}*\n"
            f"{icon} *{relative}*"
        ),
        _ctx("가격과 정확한 일일 등락률은 표시하지 않습니다."),
    ]


# ─────────────────────────────────────────────
# 1-2. 상대 강도 유틸 (_fetch_ticker_change 공용)
# ─────────────────────────────────────────────


def _fetch_ticker_change(ticker: str) -> Optional[float]:
    """
    전일 정규장 종가 대비 현재가 변동률.
    fetch_price와 동일한 원칙: timestamps로 오늘 인트라데이 바를 필터링해
    확정된 전일 종가만 prev로 사용.
    """
    _yahoo_throttle()
    resp_1d = fetch_yahoo_chart(ticker, {"interval": "1d", "range": "10d"})
    if not resp_1d:
        return None
    try:
        result_1d   = resp_1d.json()["chart"]["result"][0]
        ts_daily    = result_1d.get("timestamp", [])
        closes_raw  = result_1d["indicators"]["quote"][0].get("close", [])
    except Exception:
        return None

    now_ny = datetime.now(NY_TZ)
    today_market = now_ny.date()
    hm  = now_ny.hour * 60 + now_ny.minute
    dow = now_ny.weekday()

    if dow >= 5:                  market_state = "CLOSED"
    elif hm < 4 * 60:             market_state = "CLOSED"
    elif hm < 9 * 60 + 30:       market_state = "PRE"
    elif hm < 16 * 60:           market_state = "REGULAR"
    elif hm < 20 * 60:           market_state = "POST"
    else:                         market_state = "CLOSED"

    # 오늘 날짜 바 제거 → 확정 종가만 추출
    confirmed = [float(closes_raw[i]) for i, ts in enumerate(ts_daily)
                 if i < len(closes_raw) and closes_raw[i] is not None
                 and datetime.fromtimestamp(ts, NY_TZ).date() < today_market]

    if len(confirmed) < 1:
        # fallback: 전체 [-2] 사용
        all_c = [float(c) for c in closes_raw if c is not None]
        confirmed = all_c[:-1] if len(all_c) >= 2 else []
    if not confirmed:
        return None

    prev = confirmed[-1]  # 전일 확정 종가

    if market_state in ("PRE", "REGULAR"):
        resp_1m = fetch_yahoo_chart(ticker, {"interval": "1m", "range": "2d", "includePrePost": "true"})
        if not resp_1m:
            return None
        try:
            result_1m  = resp_1m.json()["chart"]["result"][0]
            timestamps = result_1m.get("timestamp", [])
            closes_1m  = result_1m["indicators"]["quote"][0].get("close", [])
            current    = None
            for i in range(len(timestamps) - 1, -1, -1):
                if i < len(closes_1m) and closes_1m[i] is not None:
                    if datetime.fromtimestamp(timestamps[i], NY_TZ).date() == today_market:
                        current = float(closes_1m[i])
                        break
        except Exception as e:
            log.debug(f"_fetch_ticker_change({ticker}) 1m 오류: {e}")
            return None
    else:
        # POST/CLOSED: 오늘 확정 종가 vs 전일
        all_c   = [float(c) for c in closes_raw if c is not None]
        current = all_c[-1] if all_c else None

    if not current or not prev:
        return None
    return round((current - prev) / prev * 100, 2)


# ─────────────────────────────────────────────
# 1-5. 모니터링 종목 × BTC 30일 상관계수
# ─────────────────────────────────────────────
def calc_btc_correlation() -> Optional[dict]:
    """
    모니터링 종목과 BTC-USD의 최근 30일 일간 수익률 피어슨 상관계수 계산.
    r > 0.7 → 강한 양의 상관 (BTC 따라감)
    r < 0.3 → 독립적 움직임
    """
    log.info("BTC 상관계수 계산 시작...")

    def fetch_30d_returns(ticker: str) -> Optional[list]:
        _yahoo_throttle()
        resp = fetch_yahoo_chart(ticker, {"interval": "1d", "range": "35d"})
        if not resp:
            return None
        try:
            closes = [c for c in resp.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"] if c is not None]
            if len(closes) < 31:
                return None
            return [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
        except Exception as e:
            log.debug(f"30d returns fetch ({ticker}): {e}")
            return None

    hood_r = fetch_30d_returns(TICKER)
    btc_r  = fetch_30d_returns("BTC-USD")

    if not hood_r or not btc_r:
        log.warning("BTC 상관계수: 데이터 부족")
        return None

    n = min(len(hood_r), len(btc_r))
    hood_r, btc_r = hood_r[-n:], btc_r[-n:]

    mean_h = sum(hood_r) / n
    mean_b = sum(btc_r) / n
    cov    = sum((hood_r[i] - mean_h) * (btc_r[i] - mean_b) for i in range(n)) / n
    std_h  = (sum((x - mean_h) ** 2 for x in hood_r) / n) ** 0.5
    std_b  = (sum((x - mean_b) ** 2 for x in btc_r)  / n) ** 0.5

    if std_h == 0 or std_b == 0:
        return None

    corr = round(cov / (std_h * std_b), 3)
    btc_today = _fetch_ticker_change("BTC-USD")

    if corr >= 0.7:
        signal = "high"      # BTC 강하게 추종
    elif corr >= 0.4:
        signal = "moderate"  # 중간 연동
    else:
        signal = "low"       # 독립적

    log.info(f"BTC 상관계수: r={corr} ({n}일) | BTC 당일 {btc_today:+.2f}%")
    return {"corr": corr, "signal": signal, "btc_today": btc_today, "days": n}


def format_btc_correlation_block(bd: dict) -> list:
    corr = bd["corr"]
    btc  = bd.get("btc_today", 0) or 0

    if bd["signal"] == "high":
        sig_line = f"🔗 *강한 BTC 연동* (r={corr:.2f}) — BTC 방향이 {TICKER}를 견인"
    elif bd["signal"] == "moderate":
        sig_line = f"🔗 *중간 연동* (r={corr:.2f}) — BTC와 부분적으로 동조"
    else:
        sig_line = f"🔓 *낮은 연동* (r={corr:.2f}) — {TICKER} 개별 팩터 우세"

    # BTC 방향 해석
    if bd["signal"] == "high" and btc >= 2:
        interp = f"BTC 상승 → {TICKER} 상승 압력 가능"
    elif bd["signal"] == "high" and btc <= -2:
        interp = f"BTC 하락 → {TICKER} 하락 압력 가능"
    else:
        interp = ""

    ctx = f"30일 기준 | *BTC 당일 {btc:+.2f}%*"
    if interp:
        ctx += f"  |  {interp}"

    return [
        _sec(f"*₿ {TICKER} × BTC 상관계수*\n{sig_line}"),
        _ctx(ctx),
    ]


# ─────────────────────────────────────────────
# 1-6. Apple App Store 순위 트래킹
# ─────────────────────────────────────────────
APP_STORE_ID = CONFIG.app_store_id
APP_RANK_CACHE_FILE = resolve_runtime_file(CONFIG, "app_rank_cache.json", "MONITOR_APP_RANK_CACHE_FILE")


def fetch_appstore_rank() -> Optional[dict]:
    """
    Apple App Store 미국 금융 카테고리 상위 200개 무료 앱 RSS에서
    설정된 앱 순위를 파싱.
    공식 Apple RSS: rss.applemarketingtools.com
    """
    if not APP_STORE_ID:
        log.info("App Store 순위 스킵: app_store_id 미설정")
        return None

    today = datetime.now(UTC).strftime("%Y-%m-%d")

    # 캐시 확인 (당일이면 재사용)
    if APP_RANK_CACHE_FILE.exists():
        try:
            cache = json.loads(APP_RANK_CACHE_FILE.read_text())
            if cache.get("date") == today:
                log.info(f"App Store 순위 캐시 히트: #{cache.get('rank_finance')} (Finance) | #{cache.get('rank_overall')} (Overall)")
                return cache
        except Exception:
            pass

    log.info("App Store 순위 fetch 시작...")
    result = {"date": today, "rank_finance": None, "rank_overall": None}

    # 1) 금융 카테고리 top 200
    finance_url = "https://rss.applemarketingtools.com/api/v2/us/apps/top-free/200/apps.json?genre=6015"
    resp = safe_get(finance_url)
    if resp:
        try:
            apps = resp.json()["feed"]["results"]
            for i, app in enumerate(apps):
                if app.get("id") == APP_STORE_ID:
                    result["rank_finance"] = i + 1
                    break
        except Exception as e:
            log.debug(f"App Store finance parse: {e}")

    # 2) 전체 top 200 (overall 순위용)
    overall_url = "https://rss.applemarketingtools.com/api/v2/us/apps/top-free/200/apps.json"
    resp2 = safe_get(overall_url)
    if resp2:
        try:
            apps2 = resp2.json()["feed"]["results"]
            for i, app in enumerate(apps2):
                if app.get("id") == APP_STORE_ID:
                    result["rank_overall"] = i + 1
                    break
        except Exception as e:
            log.debug(f"App Store overall parse: {e}")

    log.info(f"App Store 순위: Finance #{result['rank_finance']} | Overall #{result['rank_overall']}")
    APP_RANK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    APP_RANK_CACHE_FILE.write_text(json.dumps(result, indent=2))
    return result


def format_appstore_rank_block(prev: Optional[dict], curr: dict) -> list:
    """
    현재 순위와 전일 순위를 비교해 추이 표시.
    prev: 전일 캐시 (없으면 None)
    """
    rank_f = curr.get("rank_finance")
    rank_o = curr.get("rank_overall")

    if rank_f is None:
        return [_sec("*📱 App Store*  금융 카테고리 200위권 밖")]

    def trend(prev_r, curr_r):
        if prev_r is None or curr_r is None:
            return ""
        diff = prev_r - curr_r   # 양수 = 순위 상승 (숫자 작아짐)
        if diff >= 3:
            return f" ▲{diff}"
        elif diff <= -3:
            return f" ▼{abs(diff)}"
        return ""

    prev_f = prev.get("rank_finance") if prev else None
    prev_o = prev.get("rank_overall") if prev else None

    # FOMO 경고: 금융 top 5 진입
    fomo_warn = ""
    if rank_f is not None and rank_f <= 5:
        fomo_warn = "\n⚡ *FOMO 경보* — 금융 카테고리 Top 5 진입! 리테일 수급 유입 가능"

    lines = []
    if rank_f is not None:
        lines.append(f"💰 금융 카테고리: *#{rank_f}*{trend(prev_f, rank_f)}")
    if rank_o is not None:
        lines.append(f"📊 전체 무료 앱: *#{rank_o}*{trend(prev_o, rank_o)}")

    return [
        _sec(f"*📱 App Store 순위 (미국)*\n" + "  |  ".join(lines) + fomo_warn),
        _ctx("Apple RSS 기준 | 전일 대비 ▲상승 ▼하락"),
    ]
@dataclass
class VolumeProfile:
    poc_price: float = 0.0          # Point of Control (거래 집중 가격대)
    current_price: float = 0.0
    poc_signal: str = ""            # "resistance" | "support"
    vol_30m: int = 0                # 최근 30분 거래량
    vol_avg_30m: int = 0            # 5일 동일 시간대 평균 거래량
    vol_ratio: float = 0.0          # 현재 / 평균
    whale_detected: bool = False


def _fetch_1m_bars(ticker: str, range_str: str = "1d") -> list:
    """1분봉 데이터 반환 — [{time, open, high, low, close, volume}, ...]"""
    _yahoo_throttle()
    resp = fetch_yahoo_chart(ticker, {"interval": "1m", "range": range_str})
    if not resp:
        return []
    try:
        result = resp.json()["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        q = result["indicators"]["quote"][0]
        bars = []
        for i, ts in enumerate(timestamps):
            v = q["volume"][i]
            c = q["close"][i]
            if v is None or c is None:
                continue
            bars.append({
                "time": datetime.fromtimestamp(ts, tz=UTC),
                "close": round(c, 4),
                "volume": int(v),
            })
        return bars
    except Exception as e:
        log.debug(f"1m bars fetch error ({ticker}): {e}")
        return []


def analyze_volume_profile(current_price: float) -> Optional[VolumeProfile]:
    """
    최근 30분 1분봉 기반 POC 계산 + 5거래일 동일 시간대 평균 거래량 비교.

    POC 계산: 가격을 $0.10 단위로 버킷화 → 거래량 가중 → 최다 거래량 가격대
    거래량 비교: 5일치 1분봉에서 동일 UTC 시간 구간 평균 추출
    """
    log.info("Volume Profile 분석 시작...")

    # 당일 1분봉
    bars_1d = _fetch_1m_bars(TICKER, "1d")
    if not bars_1d:
        log.warning("1분봉 데이터 없음")
        return None

    now_utc = datetime.now(UTC)
    cutoff = now_utc - timedelta(minutes=30)
    recent_bars = [b for b in bars_1d if b["time"] >= cutoff]

    if len(recent_bars) < 5:
        log.warning(f"최근 30분 데이터 부족: {len(recent_bars)}개")
        return None

    # ── POC 계산 ──────────────────────────────
    bucket_size = 0.10   # $0.10 단위
    vol_by_price: dict = {}
    for b in recent_bars:
        bucket = round(round(b["close"] / bucket_size) * bucket_size, 2)
        vol_by_price[bucket] = vol_by_price.get(bucket, 0) + b["volume"]

    poc_price = max(vol_by_price, key=lambda k: vol_by_price[k])
    vol_30m = sum(b["volume"] for b in recent_bars)

    log.info(f"POC: ${poc_price:.2f} (현재가 ${current_price:.2f}) | 30분 거래량: {vol_30m:,}")

    # ── 5거래일 동일 시간대 평균 거래량 ──────────
    bars_5d = _fetch_1m_bars(TICKER, "5d")
    start_minute = cutoff.hour * 60 + cutoff.minute
    end_minute   = now_utc.hour * 60 + now_utc.minute

    # 오늘 날짜 제외하고 동일 시간 구간 추출
    today_date = now_utc.date()
    past_vols = []
    day_vol: dict = {}
    for b in bars_5d:
        d = b["time"].date()
        if d == today_date:
            continue
        m = b["time"].hour * 60 + b["time"].minute
        if start_minute <= m <= end_minute:
            day_vol.setdefault(d, 0)
            day_vol[d] += b["volume"]

    past_vols = list(day_vol.values())
    vol_avg_30m = int(sum(past_vols) / len(past_vols)) if past_vols else 0
    vol_ratio = round(vol_30m / vol_avg_30m, 2) if vol_avg_30m > 0 else 0.0

    log.info(f"5일 평균 동시간대 거래량: {vol_avg_30m:,} | 비율: {vol_ratio:.2f}x")

    poc_signal = "resistance" if current_price < poc_price else "support"
    whale = vol_ratio >= 1.5

    return VolumeProfile(
        poc_price=poc_price,
        current_price=current_price,
        poc_signal=poc_signal,
        vol_30m=vol_30m,
        vol_avg_30m=vol_avg_30m,
        vol_ratio=vol_ratio,
        whale_detected=whale,
    )



# ─────────────────────────────────────────────
# 1-4. 안전 마진 / 하락 모멘텀 측정 (Safety Margin)
# ─────────────────────────────────────────────
@dataclass
class SafetyMargin:
    sma20: float = 0.0
    bb_upper: float = 0.0
    bb_lower: float = 0.0
    current_price: float = 0.0
    bb_signal: str = ""
    momentum_signal: str = ""
    pct_from_lower: float = 0.0
    mom_30m_prev: float = 0.0
    mom_30m_curr: float = 0.0
    # ── 신규 필드 ──
    beta_expected_pct: float = 0.0     # β 기반 기대 수익률
    beta_excess_pct: float = 0.0       # 실제 - 기대 (음수 = 베타 초과 하락)
    divergence_warning: bool = False   # 피어 반등 중 모니터링 종목만 하락 가속
    peer_coin_pct: float = 0.0
    peer_mstr_pct: float = 0.0
    peer_changes: dict = field(default_factory=dict)
    dca_attraction: int = 0            # DCA 매력도 1~10


def check_safety_margin(
    closes_daily: list,
    current_price: float,
    actual_pct: float = 0.0,   # 모니터링 종목 당일 등락률
    beta: Optional[float] = None,
) -> Optional[SafetyMargin]:
    """
    볼린저 밴드 + 모멘텀 + 베타 초과 이탈 + 피어 분기 + DCA 매력도 통합 분석
    """
    log.info("Safety Margin 분석 시작...")

    if len(closes_daily) < 20:
        log.warning(f"볼린저 밴드 계산 불가: 데이터 {len(closes_daily)}일")
        return None

    # ── 1. 볼린저 밴드 ────────────────────────────
    window = closes_daily[-20:]
    sma20 = sum(window) / 20
    variance = sum((p - sma20) ** 2 for p in window) / 20
    std = variance ** 0.5
    bb_upper = round(sma20 + 2 * std, 2)
    bb_lower = round(sma20 - 2 * std, 2)
    pct_from_lower = round((current_price - bb_lower) / bb_lower * 100, 2)

    if current_price < bb_lower:
        bb_signal = "extreme_oversold"
    elif current_price > bb_upper:
        bb_signal = "overbought"
    elif pct_from_lower < 2:
        bb_signal = "oversold"
    else:
        bb_signal = "normal"

    log.info(f"BB: SMA20=${sma20:.2f} 하단=${bb_lower:.2f} | 현재가=${current_price:.2f} ({pct_from_lower:+.2f}%)")

    # ── 2. 30분 모멘텀 기울기 ────────────────────────
    bars = _fetch_1m_bars(TICKER, "1d")
    now_utc = datetime.now(UTC)

    def price_at(minutes_ago: int) -> Optional[float]:
        target = now_utc - timedelta(minutes=minutes_ago)
        candidates = [(abs((b["time"] - target).total_seconds()), b["close"]) for b in bars]
        if not candidates:
            return None
        closest = min(candidates, key=lambda x: x[0])
        return closest[1] if closest[0] <= 180 else None

    price_30m = price_at(30)
    price_60m = price_at(60)

    if price_30m and price_60m and price_30m > 0 and price_60m > 0:
        mom_curr = (current_price - price_30m) / price_30m * 100
        mom_prev = (price_30m - price_60m) / price_60m * 100
        if mom_curr < 0 and mom_prev < 0:
            momentum_signal = "accelerating" if mom_curr < mom_prev else "decelerating"
        elif mom_curr > 0 and mom_prev < 0:
            momentum_signal = "decelerating"
        elif abs(mom_curr) < 0.3:
            momentum_signal = "stable"
        else:
            momentum_signal = "stable"
        log.info(f"모멘텀: {mom_prev:+.2f}% → {mom_curr:+.2f}% ({momentum_signal})")
    else:
        mom_curr = mom_prev = 0.0
        momentum_signal = "stable"

    # ── 3. 베타 기반 기대 수익률 vs 실제 ───────────
    beta_expected_pct = 0.0
    beta_excess_pct = 0.0
    if beta and actual_pct != 0:
        qqq_pct = _fetch_ticker_change(BETA_BENCHMARK) or 0.0
        beta_expected_pct = round(beta * qqq_pct, 2)
        beta_excess_pct = round(actual_pct - beta_expected_pct, 2)
        log.info(f"베타 초과 이탈: 기대 {beta_expected_pct:+.2f}% vs 실제 {actual_pct:+.2f}% → 초과 {beta_excess_pct:+.2f}%")

    # ── 4. 피어 그룹 분기 감지 ──────
    peer_changes = {
        peer: (_fetch_ticker_change(peer) or 0.0)
        for peer in PEER_TICKERS
    }
    peer_values = list(peer_changes.values())
    peer_coin_pct = peer_values[0] if len(peer_values) >= 1 else 0.0
    peer_mstr_pct = peer_values[1] if len(peer_values) >= 2 else 0.0

    # 모니터링 종목이 하락 가속 중인데 피어가 모두 양전 → Divergence Warning
    divergence_warning = (
        momentum_signal == "accelerating"
        and actual_pct < -2
        and bool(peer_values)
        and all(v > 0 for v in peer_values)
    )
    if divergence_warning:
        peer_log = " / ".join(f"{p} {v:+.2f}%" for p, v in peer_changes.items())
        log.info(f"Divergence Warning: {TICKER} {actual_pct:+.2f}% 하락 가속 | {peer_log} 반등")

    # ── 5. DCA 매력도 점수 1~10 ─────────────────────
    # 3가지 요소 합산: RSI(0~4점) + BB 이탈(0~3점) + 베타 초과 이탈(0~3점)
    score = 0

    # RSI 계산 (closes_daily 재사용)
    rsi = 50.0
    if len(closes_daily) >= 15:
        from types import SimpleNamespace
        ts_tmp = get_technical_signals(closes_daily)
        rsi = ts_tmp.rsi_14

    if rsi <= 25:
        score += 4
    elif rsi <= 30:
        score += 3
    elif rsi <= 40:
        score += 2
    elif rsi <= 50:
        score += 1

    if bb_signal == "extreme_oversold":
        score += 3
    elif bb_signal == "oversold":
        score += 2
    elif pct_from_lower < 5:
        score += 1

    # 베타 초과 이탈: 기대보다 더 빠졌을수록 통계적 반등 가능성
    if beta_excess_pct <= -5:
        score += 3
    elif beta_excess_pct <= -3:
        score += 2
    elif beta_excess_pct <= -1:
        score += 1

    # 하락 가속 중이면 감점
    if momentum_signal == "accelerating":
        score = max(0, score - 1)
    if divergence_warning:
        score = max(0, score - 1)

    dca_attraction = max(1, min(10, score))
    log.info(f"DCA 매력도: {dca_attraction}/10 (RSI={rsi:.1f}, BB={bb_signal}, β초과={beta_excess_pct:+.2f}%)")

    return SafetyMargin(
        sma20=round(sma20, 2),
        bb_upper=bb_upper,
        bb_lower=bb_lower,
        current_price=current_price,
        bb_signal=bb_signal,
        momentum_signal=momentum_signal,
        pct_from_lower=pct_from_lower,
        mom_30m_prev=round(mom_prev, 2),
        mom_30m_curr=round(mom_curr, 2),
        beta_expected_pct=beta_expected_pct,
        beta_excess_pct=beta_excess_pct,
        divergence_warning=divergence_warning,
        peer_coin_pct=peer_coin_pct,
        peer_mstr_pct=peer_mstr_pct,
        peer_changes=peer_changes,
        dca_attraction=dca_attraction,
    )


# ─────────────────────────────────────────────
# 2. 뉴스 (BUG 2 FIX: 관련성 필터 + 한국어 강제)
# ─────────────────────────────────────────────
def fetch_news() -> list:
    _yahoo_throttle()
    url = YAHOO_RSS_URL.format(ticker=TICKER)
    resp = safe_get(url)
    if not resp:
        _set_source_health("Yahoo 뉴스", "실패")
        return []
    try:
        root = ET.fromstring(resp.text)
        news = []
        for item in root.findall(".//item")[:15]:
            title    = item.findtext("title", "")
            pub_date = item.findtext("pubDate", "")
            link     = item.findtext("link", "")
            try:
                from urllib.parse import urlparse
                source = urlparse(link).netloc.lower().removeprefix("www.")
            except Exception:
                source = ""
            news.append({
                "title": title,
                "date":  pub_date,
                "link":  link,
                "source": source or "Yahoo Finance",
                "hash":  hashlib.md5(title.encode()).hexdigest()[:12],
            })
        _set_source_health("Yahoo 뉴스", "정상")
        return news
    except Exception as e:
        log.error(f"News parse error: {e}")
        _set_source_health("Yahoo 뉴스", "실패")
        return []


def _fetch_article_body(url: str, max_chars: int = 600) -> str:
    """기사 URL에서 본문 앞부분 발췌. 페이월/차단 사이트는 조용히 스킵."""
    if not url:
        return ""

    # 페이월/스크래핑 차단 도메인 스킵
    BLOCKED_DOMAINS = {
        "investopedia.com", "thestreet.com", "wsj.com", "ft.com",
        "bloomberg.com", "barrons.com", "marketwatch.com",
    }
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lstrip("www.")
        if any(domain.endswith(d) for d in BLOCKED_DOMAINS):
            return ""
    except Exception:
        return ""

    try:
        import re
        resp = safe_get(url, timeout=8, retries=1)
        if not resp:
            return ""
        text = re.sub(r"<script[^>]*>.*?</script>", "", resp.text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>",   "", text,      flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        sentences = [s.strip() for s in text.split(".") if len(s.strip()) > 40]
        return ". ".join(sentences[:6])[:max_chars]
    except Exception:
        return ""


def translate_news(news: list) -> list:
    """
    설정 종목 관련 뉴스 필터링 + 한국어 요약(15자) + 기사 핵심 번역(2~3문장)
    """
    if not news:
        return news
    for item in news:
        item["analysis_status"] = "pending"
    if not ANTHROPIC_API_KEY:
        log.info("translate_news skip — ANTHROPIC_API_KEY 없음")
        for item in news:
            item["analysis_status"] = "unavailable"
        _set_source_health("AI 뉴스", "키 없음")
        return news

    # 기사 본문 발췌 (관련성 판단 전 미리 fetch)
    for n in news:
        n["body"] = _fetch_article_body(n.get("link", ""))

    # 제목 + 본문 발췌 함께 전달
    articles = []
    for i, n in enumerate(news):
        entry = f"{i+1}. [제목] {n['title']}"
        if n.get("body"):
            entry += f"\n   [본문] {n['body']}"
        articles.append(entry)
    content = "\n\n".join(articles)

    log.info(f"Claude API 호출: 뉴스 번역 ({len(news)}건, 본문 포함)")
    profile_terms = ", ".join(NEWS_TERMS)
    priority_terms = ", ".join(PRIORITY_KEYWORDS)
    risk_terms = ", ".join(RISK_KEYWORDS)
    kpis = ", ".join(CORE_KPIS)
    aliases = ", ".join(COMPANY_ALIASES)
    prompt = f"""당신은 {DISPLAY_TICKER}({COMPANY_NAME or TICKER}) 장기투자 논지 모니터입니다.
아래 뉴스 목록(제목+본문 발췌)을 분석해주세요.

회사 별칭: {aliases}
사업 맥락: {PROFILE_CONTEXT}
핵심 테마: {profile_terms}
중요 이벤트·KPI: {priority_terms}, {kpis}
핵심 위험: {risk_terms}

규칙:
1. {COMPANY_NAME or TICKER} / {DISPLAY_TICKER} 주가에 직접 영향을 주는 뉴스만 포함
2. 포함 기준: 실적, 가이던스, 수주, backlog, 마진, 현금흐름, 생산능력, 규제, 경쟁, 경영진, 주요 제품, 소송
3. 제외 기준: 업종 일반 뉴스, 금리 일반론, 다른 회사 뉴스에 {TICKER}가 언급만 된 경우
4. 기사에 없는 사실을 추정하지 말고 사실과 해석을 분리
5. 반드시 한국어로만 출력
6. 현재 주가, 목표주가, 정확한 일일 등락률은 출력하지 말 것

각 뉴스에 대해 JSON 배열로만 응답 (다른 텍스트 없이):
[
  {{
    "idx": 1,
    "relevant": true,
    "summary": "15자 이내 핵심 요약",
    "translation": "기사에서 확인되는 핵심 사실 2~3문장. 중요한 수치와 전후 변화 포함.",
    "sentiment": "positive|negative|neutral",
    "thesis_impact": "strengthen|neutral|risk|damage",
    "impact_reason": "투자 논지에 미치는 이유 한 문장",
    "confidence": "high|medium|low"
  }},
  {{"idx": 2, "relevant": false}}
]

뉴스 목록:
{content}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 3000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=40,
        )
        if resp.status_code != 200:
            log.error(f"Claude API 오류 (뉴스 번역): HTTP {resp.status_code} — {resp.text[:200]}")
            for item in news:
                item["analysis_status"] = "failed"
            _set_source_health("AI 뉴스", "실패")
            return news

        text = ""
        for block in resp.json().get("content", []):
            if block.get("type") == "text":
                text += block["text"]
        text = text.strip().replace("```json", "").replace("```", "").strip()
        results = json.loads(text)

        if not isinstance(results, list):
            raise ValueError("뉴스 분석 응답이 JSON 배열이 아님")

        for item in news:
            item["analysis_status"] = "failed"
            item["skip"] = True

        analyzed_indices = set()
        relevant_count = 0
        for item in results:
            try:
                idx = int(item.get("idx", 0)) - 1
            except (TypeError, ValueError):
                continue
            if not (0 <= idx < len(news)):
                continue
            is_relevant = item.get("relevant")
            if not isinstance(is_relevant, bool):
                continue
            if is_relevant:
                summary = str(item.get("summary", "")).strip()
                translation = str(item.get("translation", "")).strip()
                impact_reason = str(item.get("impact_reason", "")).strip()
                thesis_impact = item.get("thesis_impact", "")
                if (
                    not summary
                    or not translation
                    or not impact_reason
                    or thesis_impact not in {"strengthen", "neutral", "risk", "damage"}
                ):
                    continue
            analyzed_indices.add(idx)
            news[idx]["analysis_status"] = "success"
            if not is_relevant:
                news[idx]["skip"] = True
            else:
                relevant_count += 1
                news[idx]["skip"] = False
                news[idx]["summary"]     = summary
                news[idx]["translation"] = translation
                news[idx]["sentiment"]   = item.get("sentiment", "neutral")
                news[idx]["thesis_impact"] = thesis_impact
                news[idx]["impact_reason"] = impact_reason
                news[idx]["confidence"] = item.get("confidence", "medium")
        log.info(
            f"뉴스 번역 완료: 응답 {len(results)}건 — "
            f"관련 {relevant_count}건 / 분석 완료 {len(analyzed_indices)}건"
        )
        missing = len(news) - len(analyzed_indices)
        if missing:
            log.warning(f"뉴스 분석 응답 누락: {missing}건 — 다음 실행에서 재시도")
            _set_source_health("AI 뉴스", f"부분 실패 ({missing}건 재시도)")
        else:
            _set_source_health("AI 뉴스", "정상")
    except Exception as e:
        log.warning(f"뉴스 번역 예외: {e}")
        for item in news:
            item["analysis_status"] = "failed"
        _set_source_health("AI 뉴스", "실패")

    return news


def analyzed_news_hashes(news: list) -> list:
    """AI 분석이 완료된 뉴스만 중복 방지 상태에 반영한다."""
    return [item["hash"] for item in news
            if item.get("analysis_status") == "success" and item.get("hash")]


# ─────────────────────────────────────────────
# 3. RSI / MACD
# ─────────────────────────────────────────────
def calculate_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def calculate_macd(closes: list) -> tuple:
    if len(closes) < 35:
        return 0.0, 0.0, 0.0
    def ema(data, p):
        k = 2 / (p + 1)
        r = [data[0]]
        for i in range(1, len(data)):
            r.append(data[i] * k + r[-1] * (1 - k))
        return r
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    macd = [e12[i] - e26[i] for i in range(len(closes))]
    sig = ema(macd, 9)
    return round(macd[-1], 4), round(sig[-1], 4), round(macd[-1] - sig[-1], 4)


def get_technical_signals(closes: list) -> TechnicalSignals:
    rsi = calculate_rsi(closes)
    macd_line, macd_sig, macd_hist = calculate_macd(closes)
    ts = TechnicalSignals(rsi_14=rsi, macd_line=macd_line, macd_signal=macd_sig, macd_histogram=macd_hist)
    if rsi <= 30:
        ts.rsi_alert = "oversold"
    elif rsi >= 70:
        ts.rsi_alert = "overbought"
    if len(closes) >= 36:
        mp, sp, _ = calculate_macd(closes[:-1])
        if mp < sp and macd_line > macd_sig:
            ts.macd_alert = "bullish_cross"
        elif mp > sp and macd_line < macd_sig:
            ts.macd_alert = "bearish_cross"
    return ts


# ─────────────────────────────────────────────
# DCA 기술지표 점수 시스템 v2 (5-Layer, 100pts)
# ─────────────────────────────────────────────

def fetch_ohlcv(days: int = 210) -> dict:
    """일봉 OHLCV 반환 — closes/opens/highs/lows/volumes"""
    _yahoo_throttle()
    resp = fetch_yahoo_chart(TICKER, {"interval": "1d", "range": f"{days}d"})
    if not resp:
        return {}
    try:
        result = resp.json()["chart"]["result"][0]
        q = result["indicators"]["quote"][0]
        def _clean(lst):
            return [v if v is not None else 0.0 for v in lst]
        return {
            "closes":  _clean(q.get("close", [])),
            "opens":   _clean(q.get("open", [])),
            "highs":   _clean(q.get("high", [])),
            "lows":    _clean(q.get("low", [])),
            "volumes": [int(v) if v else 0 for v in q.get("volume", [])],
        }
    except Exception as e:
        log.error(f"fetch_ohlcv error: {e}")
        return {}


def fetch_weekly_ohlcv(weeks: int = 40) -> dict:
    """주봉 OHLCV 반환 (MACD 계산에 최소 35주 필요)"""
    _yahoo_throttle()
    resp = fetch_yahoo_chart(TICKER, {"interval": "1wk", "range": f"{weeks * 7 + 14}d"})
    if not resp:
        return {}
    try:
        result = resp.json()["chart"]["result"][0]
        q = result["indicators"]["quote"][0]
        def _clean(lst):
            return [v if v is not None else 0.0 for v in lst]
        return {
            "closes":  _clean(q.get("close", [])),
            "highs":   _clean(q.get("high", [])),
            "lows":    _clean(q.get("low", [])),
            "volumes": [int(v) if v else 0 for v in q.get("volume", [])],
        }
    except Exception as e:
        log.error(f"fetch_weekly_ohlcv error: {e}")
        return {}


def _calc_ema_series(data: list, period: int) -> list:
    """EMA 시리즈 반환"""
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for v in data[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _calc_obv(closes: list, volumes: list) -> list:
    """OBV(On-Balance Volume) 시리즈"""
    if len(closes) < 2 or len(volumes) < 2:
        return []
    obv = [0]
    for i in range(1, min(len(closes), len(volumes))):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    return obv


def _calc_mfi(highs: list, lows: list, closes: list,
              volumes: list, period: int = 14) -> Optional[float]:
    """Money Flow Index (거래량 가중 RSI)"""
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 1:
        return None
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
    pos_mf = neg_mf = 0.0
    for i in range(n - period, n):
        mf = typical[i] * volumes[i]
        if typical[i] > typical[i - 1]:
            pos_mf += mf
        else:
            neg_mf += mf
    if neg_mf == 0:
        return 100.0
    return round(100 - 100 / (1 + pos_mf / neg_mf), 2)


def _calc_stochastic(highs: list, lows: list, closes: list,
                     period: int = 14, smooth_k: int = 3,
                     smooth_d: int = 3) -> tuple:
    """Stochastic %K, %D 반환"""
    n = min(len(highs), len(lows), len(closes))
    if n < period + smooth_k + smooth_d:
        return None, None
    raw_k = []
    for i in range(period - 1, n):
        hh = max(highs[i - period + 1: i + 1])
        ll = min(lows[i - period + 1: i + 1])
        raw_k.append(((closes[i] - ll) / (hh - ll) * 100) if hh != ll else 50.0)
    if len(raw_k) < smooth_k:
        return None, None
    k_series = [sum(raw_k[i - smooth_k + 1: i + 1]) / smooth_k
                for i in range(smooth_k - 1, len(raw_k))]
    if len(k_series) < smooth_d:
        return None, None
    d_val = sum(k_series[-smooth_d:]) / smooth_d
    return round(k_series[-1], 2), round(d_val, 2)


def _calc_atr(highs: list, lows: list, closes: list,
              period: int = 14) -> Optional[float]:
    """ATR(14)"""
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    tr_list = [max(
        highs[i] - lows[i],
        abs(highs[i] - closes[i - 1]),
        abs(lows[i] - closes[i - 1]),
    ) for i in range(1, n)]
    if len(tr_list) < period:
        return None
    return round(sum(tr_list[-period:]) / period, 4)


def _detect_rsi_bullish_divergence(closes: list, lookback: int = 20) -> bool:
    """
    강세 다이버전스: 최근 lookback봉 안에서
    가격은 더 낮은 저점, RSI는 더 높은 저점이면 True
    """
    if len(closes) < lookback + 14:
        return False
    window = closes[-(lookback + 14):]
    rsi_series = [calculate_rsi(window[:i + 1]) for i in range(14, len(window))]
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


def _calc_cmf(highs: list, lows: list, closes: list,
              volumes: list, period: int = 21) -> Optional[float]:
    """
    Chaikin Money Flow (CMF) — 기관 매집/매도 강도 측정.
    종가가 일봉 range 상단에 가까울수록 + 거래량이 많을수록 양수.
    prop-desk에서 스마트머니 추적에 사용.
    """
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period:
        return None
    mfv_sum = vol_sum = 0.0
    for i in range(n - period, n):
        hl = highs[i] - lows[i]
        if hl == 0:
            continue
        mfm = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / hl
        mfv_sum += mfm * volumes[i]
        vol_sum += volumes[i]
    return round(mfv_sum / vol_sum, 4) if vol_sum else 0.0


def _calc_daily_hvn(highs: list, lows: list, closes: list,
                    volumes: list, lookback: int = 60) -> Optional[float]:
    """
    Daily High Volume Node (HVN) — 일봉 기반 Volume Profile POC 근사.
    현재가 기준 0.5% 버킷으로 거래량 집중 가격대를 탐색.
    반환: 현재가 대비 HVN 거리(%), 음수=HVN이 아래(지지), 양수=HVN이 위(저항)
    """
    n = min(len(highs), len(lows), len(closes), len(volumes))
    lookback = min(lookback, n)
    if lookback < 20:
        return None
    cur = closes[-1]
    if cur <= 0:
        return None
    bucket_size = 0.005  # 0.5% 버킷
    vol_by_bucket: dict = {}
    for i in range(n - lookback, n):
        tp = (highs[i] + lows[i] + closes[i]) / 3
        pct = (tp / cur - 1)
        bucket = round(pct / bucket_size) * bucket_size
        vol_by_bucket[bucket] = vol_by_bucket.get(bucket, 0) + volumes[i]
    if not vol_by_bucket:
        return None
    hvn_bucket = max(vol_by_bucket, key=vol_by_bucket.get)
    return round(hvn_bucket * 100, 2)


def calculate_dca_technical_score(
    ohlcv: dict,
    weekly_ohlcv: dict,
    sm=None,      # SafetyMargin (Optional)
) -> Optional[DCATechnicalScore]:
    """
    5-Layer 100점 DCA 기술지표 점수.
    각 항목마다 한글 현황 설명 포함.
    """
    if not ohlcv or len(ohlcv.get("closes", [])) < 30:
        log.warning("DCA technical score: OHLCV 데이터 부족")
        return None

    closes  = ohlcv["closes"]
    highs   = ohlcv["highs"]
    lows    = ohlcv["lows"]
    volumes = ohlcv["volumes"]
    layers  = []

    # ════════════════════════
    # A. Volume / Flow (35pts)
    # ════════════════════════
    items_a: list = []

    # A1. OBV Divergence (10pts)
    obv = _calc_obv(closes, volumes)
    if len(obv) >= 6:
        p_chg = closes[-1] - closes[-6]
        o_chg = obv[-1] - obv[-6]
        if p_chg < 0 and o_chg > 0:
            a1, desc = 10, "가격 5일 하락 중 OBV 상승 → 스마트머니 매집 감지 🟢"
        elif p_chg < 0 and o_chg < 0:
            a1, desc = 0,  "가격·OBV 동반 하락 → 매도 압력 지속 🔴"
        elif p_chg > 0 and o_chg > 0:
            a1, desc = 5,  "가격·OBV 동반 상승 → 정상 추세, 눌림 아님 ⚪"
        else:
            a1, desc = 2,  "가격 상승 중 OBV 약화 → 상승 추세 신뢰도 낮음 🟡"
    else:
        a1, desc = 0, "데이터 부족"
    items_a.append(DCAScoreItem("OBV Divergence", a1, 10, desc))

    # A2. MFI(14) (10pts)
    mfi = _calc_mfi(highs, lows, closes, volumes)
    if mfi is not None:
        if mfi < 20:
            a2, desc = 10, f"MFI {mfi:.1f} — 극과매도, 스마트머니 유입 임박 🟢"
        elif mfi < 30:
            a2, desc = 7,  f"MFI {mfi:.1f} — 과매도 구간, 매수세 증가 가능 🟢"
        elif mfi < 40:
            a2, desc = 4,  f"MFI {mfi:.1f} — 중립 하단, 소폭 매수 우호 🟡"
        elif mfi > 80:
            a2, desc = 0,  f"MFI {mfi:.1f} — 과매수, 차익 실현 압력 🔴"
        else:
            a2, desc = 1,  f"MFI {mfi:.1f} — 중립 구간 ⚪"
    else:
        a2, desc = 0, "MFI 계산 불가"
    items_a.append(DCAScoreItem("MFI(14)", a2, 10, desc))

    # A3. Volume Contraction (8pts)
    if len(volumes) >= 20:
        vol_3d  = sum(v for v in volumes[-3:] if v > 0) / 3
        vol_20d = sum(v for v in volumes[-20:] if v > 0) / 20
        ratio = vol_3d / vol_20d if vol_20d > 0 else 1.0
        pct = ratio * 100
        if ratio < 0.70:
            a3, desc = 8, f"최근 3일 거래량 {pct:.0f}% (20일 평균 대비) — 셀링 약화, 건강한 눌림 🟢"
        elif ratio < 0.85:
            a3, desc = 5, f"최근 3일 거래량 {pct:.0f}% — 소폭 수축, 보통 눌림 🟡"
        elif ratio < 1.00:
            a3, desc = 2, f"최근 3일 거래량 {pct:.0f}% — 평균 수준, 눌림 신호 약함 ⚪"
        else:
            a3, desc = 0, f"최근 3일 거래량 {pct:.0f}% — 거래량 급증, 패닉/돌파 점검 필요 🔴"
    else:
        a3, desc = 0, "데이터 부족"
    items_a.append(DCAScoreItem("Volume Contraction", a3, 8, desc))

    # A4. CMF — Chaikin Money Flow (7pts) [Whale Flow 대체]
    # 1분봉 의존 제거 → 일봉 OHLCV로 기관 매집 강도 정량화
    cmf = _calc_cmf(highs, lows, closes, volumes)
    if cmf is not None:
        if cmf > 0.15:
            a4, desc = 7, f"CMF {cmf:+.3f} — 강한 기관 매집 신호 (스마트머니 유입) 🟢"
        elif cmf > 0.05:
            a4, desc = 5, f"CMF {cmf:+.3f} — 매수 우세, 기관 자금 유입 중 🟢"
        elif cmf > -0.05:
            a4, desc = 2, f"CMF {cmf:+.3f} — 중립, 매수/매도 균형 ⚪"
        elif cmf > -0.15:
            a4, desc = 1, f"CMF {cmf:+.3f} — 매도 우세, 기관 이탈 🟡"
        else:
            a4, desc = 0, f"CMF {cmf:+.3f} — 강한 기관 매도 압력 🔴"
    else:
        a4, desc = 2, "CMF 데이터 부족 ⚪"
    items_a.append(DCAScoreItem("CMF(21) 기관매집", a4, 7, desc))

    layers.append(DCALayerScore(
        "Volume / Flow", "A",
        sum(i.score for i in items_a), 35, items_a))

    # ═══════════════════
    # B. Trend (25pts)
    # ═══════════════════
    items_b: list = []

    # B1. MACD Histogram 바닥 수렴 (10pts)
    if len(closes) >= 35:
        ml, ms, mh = calculate_macd(closes)
        # 최근 3봉 히스토그램 방향 (인덱스 슬라이싱으로 직접 계산)
        hist_prev = [calculate_macd(closes[:-(3 - j)])[2] for j in range(3)]
        converging = all(hist_prev[j] > hist_prev[j - 1] for j in range(1, 3)) and mh < 0
        if converging:
            b1, desc = 10, f"MACD 히스토그램 {mh:.4f} — 음수 구간 3봉 수렴 (바닥 탐색) 🟢"
        elif mh > 0 and all(hist_prev[j] > hist_prev[j - 1] for j in range(1, 3)):
            b1, desc = 7,  f"MACD 히스토그램 {mh:.4f} — 양수 상승 중, 모멘텀 강화 🟡"
        elif mh > 0:
            b1, desc = 5,  f"MACD 히스토그램 {mh:.4f} — 양수 구간 ⚪"
        else:
            b1, desc = 2,  f"MACD 히스토그램 {mh:.4f} — 음수 발산 중, 하락 모멘텀 🔴"
    else:
        b1, desc = 2, "데이터 부족 (35봉 이상 필요)"
    items_b.append(DCAScoreItem("MACD Histogram 수렴", b1, 10, desc))

    # B2. EMA 구조 (10pts)
    e20 = _calc_ema_series(closes, 20)
    e50 = _calc_ema_series(closes, 50)
    e200 = _calc_ema_series(closes, 200)
    cur = closes[-1]
    if e20 and e50 and e200:
        v20, v50, v200 = e20[-1], e50[-1], e200[-1]
        if cur < v20 and v20 > v50 > v200:
            b2, desc = 10, f"가격 < 20EMA < 50EMA < 200EMA 순서 유지 → 장기 추세 건강, 눌림 구간 🟢"
        elif cur < v50 and v50 > v200:
            b2, desc = 7,  f"가격 50EMA 아래, 200EMA 위 → 중기 조정, 장기 추세 양호 🟡"
        elif cur > v20 > v50:
            b2, desc = 5,  f"가격 > 20EMA > 50EMA → 상승 추세, 눌림 아님 ⚪"
        elif v20 < v50:
            b2, desc = 2,  f"20EMA < 50EMA (데드크로스 접근) → 하락 추세 전환 주의 🔴"
        else:
            b2, desc = 4,  "EMA 혼재 — 명확한 추세 없음 ⚪"
    elif e20 and e50:
        v20, v50 = e20[-1], e50[-1]
        b2 = 7 if cur < v20 and v20 > v50 else 3
        desc = f"가격 < 20EMA, 20>50 (200EMA 미계산) 🟡" if b2 == 7 else f"EMA 50일까지 계산 ⚪"
    else:
        b2, desc = 3, "데이터 부족 (최소 50봉 필요)"
    items_b.append(DCAScoreItem("EMA 구조 (20/50/200)", b2, 10, desc))

    # B3. Daily HVN 거리 (5pts) [인트라데이 POC 대체]
    # 60일 일봉 거래량 분포에서 High Volume Node 계산 → 현재가와의 거리
    hvn_pct = _calc_daily_hvn(highs, lows, closes, volumes)
    if hvn_pct is not None:
        if hvn_pct < -3:
            b3, desc = 5, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 아래, 역사적 지지 구간 🟢"
        elif hvn_pct < -1:
            b3, desc = 4, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 하단 근접, 가치 구간 🟢"
        elif hvn_pct < 1:
            b3, desc = 2, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 근처, 중립 ⚪"
        elif hvn_pct < 4:
            b3, desc = 1, f"Daily HVN 대비 {hvn_pct:+.1f}% 위 — HVN이 하단 지지 🟡"
        else:
            b3, desc = 0, f"Daily HVN 대비 {hvn_pct:+.1f}% 위 — 매물대 상단, 고평가 🔴"
    else:
        b3, desc = 2, "Daily HVN 계산 불가 (데이터 부족) ⚪"
    items_b.append(DCAScoreItem("Daily HVN 거리", b3, 5, desc))

    layers.append(DCALayerScore(
        "Trend", "B",
        sum(i.score for i in items_b), 25, items_b))

    # ══════════════════════
    # C. Momentum (20pts)
    # ══════════════════════
    items_c: list = []

    # C1. RSI(14) 일봉 (8pts)
    rsi_val = calculate_rsi(closes)
    if rsi_val <= 25:
        c1, desc = 8, f"RSI {rsi_val:.1f} — 극과매도 구간 🟢"
    elif rsi_val <= 30:
        c1, desc = 7, f"RSI {rsi_val:.1f} — 과매도 구간 🟢"
    elif rsi_val <= 40:
        c1, desc = 5, f"RSI {rsi_val:.1f} — 약세 구간 🟡"
    elif rsi_val <= 50:
        c1, desc = 2, f"RSI {rsi_val:.1f} — 중립 하단 ⚪"
    elif rsi_val >= 70:
        c1, desc = 0, f"RSI {rsi_val:.1f} — 과매수, 추격 매수 위험 🔴"
    else:
        c1, desc = 1, f"RSI {rsi_val:.1f} — 중립 ⚪"
    items_c.append(DCAScoreItem("RSI(14) 일봉", c1, 8, desc))

    # C2. Stochastic(14,3,3) (7pts)
    sk, sd = _calc_stochastic(highs, lows, closes)
    if sk is not None and sd is not None:
        in_os = sk < 20 and sd < 20
        crossed = sk > sd
        if in_os and crossed:
            c2, desc = 7, f"Stoch %K={sk:.1f} %D={sd:.1f} — 과매도 골든크로스 🟢"
        elif in_os:
            c2, desc = 4, f"Stoch %K={sk:.1f} %D={sd:.1f} — 과매도, 크로스 대기 🟡"
        elif sk < 50 and crossed:
            c2, desc = 2, f"Stoch %K={sk:.1f} %D={sd:.1f} — 중립대 골든크로스 ⚪"
        elif sk > 80:
            c2, desc = 0, f"Stoch %K={sk:.1f} — 과매수 🔴"
        else:
            c2, desc = 1, f"Stoch %K={sk:.1f} %D={sd:.1f} — 중립 ⚪"
    else:
        c2, desc = 1, "데이터 부족 ⚪"
    items_c.append(DCAScoreItem("Stochastic(14,3,3)", c2, 7, desc))

    # C3. RSI Bullish Divergence (5pts)
    has_div = _detect_rsi_bullish_divergence(closes)
    if has_div:
        c3, desc = 5, "강세 다이버전스 확인 — 가격 저점 갱신, RSI 저점 상승 → 추세 전환 신호 🟢"
    else:
        c3, desc = 0, "강세 다이버전스 미감지 — 현재 없음 ⚪"
    items_c.append(DCAScoreItem("RSI Bullish Divergence", c3, 5, desc))

    layers.append(DCALayerScore(
        "Momentum", "C",
        sum(i.score for i in items_c), 20, items_c))

    # ══════════════════════════════
    # D. Volatility / Entry (12pts)
    # ══════════════════════════════
    items_d: list = []

    # D1. 볼린저 밴드 위치 (7pts) — SafetyMargin 연동
    if sm:
        if sm.bb_signal == "extreme_oversold":
            d1, desc = 7, f"BB 하단 이탈 (하단 대비 {sm.pct_from_lower:+.1f}%) — 극과매도, 평균회귀 압력 🟢"
        elif sm.bb_signal == "oversold":
            d1, desc = 5, f"BB 하단 근접 ({sm.pct_from_lower:+.1f}%) — 과매도 진입 구간 🟡"
        elif sm.bb_signal == "overbought":
            d1, desc = 0, "BB 상단 돌파 — 과열 구간, 매수 자제 🔴"
        else:
            d1, desc = 2, f"BB 중단 구간 ({sm.pct_from_lower:+.1f}%) — 중립 ⚪"
    elif len(closes) >= 20:
        sma20 = sum(closes[-20:]) / 20
        std20 = (sum((c - sma20) ** 2 for c in closes[-20:]) / 20) ** 0.5
        lower = sma20 - 2 * std20
        pfl = (closes[-1] - lower) / lower * 100 if lower > 0 else 0
        if closes[-1] < lower:
            d1, desc = 7, f"BB 하단 이탈 ({pfl:+.1f}%) — 극과매도 🟢"
        elif pfl < 3:
            d1, desc = 5, f"BB 하단 근접 ({pfl:+.1f}%) 🟡"
        else:
            d1, desc = 2, f"BB 중단 ({pfl:+.1f}%) ⚪"
    else:
        d1, desc = 2, "데이터 부족"
    items_d.append(DCAScoreItem("볼린저 밴드 위치", d1, 7, desc))

    # D2. ATR 맥락 (5pts)
    atr = _calc_atr(highs, lows, closes)
    if atr and atr > 0 and len(closes) >= 20:
        recent_high = max(closes[-20:])
        drawdown = abs(closes[-1] - recent_high)
        atr_mult = drawdown / atr
        if atr_mult < 1.5:
            d2, desc = 5, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 건강한 조정 범위 🟢"
        elif atr_mult < 2.5:
            d2, desc = 2, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 확대 조정 🟡"
        else:
            d2, desc = 0, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 과도한 하락, 원인 점검 필요 🔴"
    else:
        d2, desc = 2, "ATR 계산 불가 ⚪"
    items_d.append(DCAScoreItem("ATR 맥락", d2, 5, desc))

    layers.append(DCALayerScore(
        "Volatility / Entry", "D",
        sum(i.score for i in items_d), 12, items_d))

    # ════════════════════════════════════
    # E. Multi-Timeframe Confluence (8pts)
    # ════════════════════════════════════
    items_e: list = []
    w_closes = weekly_ohlcv.get("closes", []) if weekly_ohlcv else []

    # E1. 주봉 RSI (4pts)
    if len(w_closes) >= 14:
        wrsi = calculate_rsi(w_closes)
        if wrsi < 30:
            e1, desc = 4, f"주봉 RSI {wrsi:.1f} — 주봉 과매도, 중기 반등 구간 🟢"
        elif wrsi < 40:
            e1, desc = 3, f"주봉 RSI {wrsi:.1f} — 주봉 과매도 접근 🟡"
        elif wrsi < 50:
            e1, desc = 2, f"주봉 RSI {wrsi:.1f} — 중립 하단 ⚪"
        elif wrsi >= 70:
            e1, desc = 0, f"주봉 RSI {wrsi:.1f} — 주봉 과매수 🔴"
        else:
            e1, desc = 1, f"주봉 RSI {wrsi:.1f} — 중립 ⚪"
    else:
        e1, desc = 1, "주봉 데이터 부족 ⚪"
    items_e.append(DCAScoreItem("주봉 RSI", e1, 4, desc))

    # E2. 주봉 MACD Histogram (4pts)
    if len(w_closes) >= 35:
        wml, wms, wmh = calculate_macd(w_closes)
        if wmh > 0:
            e2, desc = 4, f"주봉 MACD 히스토그램 {wmh:.4f} — 양전환, 주봉 모멘텀 상승 🟢"
        else:
            # 직전봉 히스토그램으로 수렴 여부 확인
            prev_wmh = calculate_macd(w_closes[:-1])[2] if len(w_closes) > 35 else wmh
            if wmh > prev_wmh:
                e2, desc = 3, f"주봉 MACD 히스토그램 {wmh:.4f} — 음수이나 수렴 중 🟡"
            else:
                e2, desc = 0, f"주봉 MACD 히스토그램 {wmh:.4f} — 음수 발산 중 🔴"
    else:
        e2, desc = 1, "주봉 데이터 부족 (35주 이상 필요) ⚪"
    items_e.append(DCAScoreItem("주봉 MACD", e2, 4, desc))

    layers.append(DCALayerScore(
        "Multi-Timeframe", "E",
        sum(i.score for i in items_e), 8, items_e))

    # ═════════════════
    # 총점 + 등급
    # ═════════════════
    total = max(0, min(100, sum(l.pts for l in layers)))
    if total >= 80:
        grade, grade_emoji = "Strong Technical Support", "🟢🟢"
    elif total >= 60:
        grade, grade_emoji = "Technical Support", "🟢"
    elif total >= 40:
        grade, grade_emoji = "Neutral", "⚪"
    elif total >= 20:
        grade, grade_emoji = "Caution", "🟡"
    else:
        grade, grade_emoji = "Technical Risk", "🔴"

    log.info(f"DCA 기술지표 점수: {total}/100 ({grade})")
    return DCATechnicalScore(layers=layers, total=total,
                             grade=grade, grade_emoji=grade_emoji)


def format_dca_technical_block(score: DCATechnicalScore) -> list:
    """
    5-Layer DCA 기술지표 점수 Slack 블록.
    각 레이어별로 분리, 항목마다 (점수/최대) 형식 표기.
    """
    layer_emoji = {"A": "📊", "B": "📈", "C": "⚡", "D": "🎯", "E": "🔭"}
    fill = int(score.total / 100 * 10)
    bar = "█" * fill + "░" * (10 - fill)

    if score.total >= 80:
        condition = "눌림 신호 강함"
    elif score.total >= 60:
        condition = "눌림 신호 있음"
    elif score.total >= 40:
        condition = "기술 신호 혼재"
    elif score.total >= 20:
        condition = "진입 신호 약함"
    else:
        condition = "기술 위험 우세"

    blocks = [_sec(
        f"*🧮 DCA 보조 기술환경: {score.total}/100*  {condition}\n`{bar}`"
    )]

    for layer in score.layers:
        emoji = layer_emoji.get(layer.layer_id, "•")
        lines = [f"*{emoji} {layer.layer_id}. {layer.name}  —  {layer.pts}/{layer.max_pts}pts*"]
        for item in layer.items:
            lines.append(f"({item.score}/{item.max_pts}) *{item.label}*: {item.desc}")
        blocks.append(_ctx("\n".join(lines)))

    blocks.append(_ctx("기술지표는 기존 DCA 계획을 변경하는 단독 근거가 아닙니다."))
    return blocks


# ─────────────────────────────────────────────
# 4. 옵션 PCR (Yahoo crumb 인증 + CBOE fallback)
# ─────────────────────────────────────────────
_yahoo_session: Optional[requests.Session] = None
_yahoo_crumb: str = ""


def _get_yahoo_crumb() -> tuple:
    """Yahoo 쿠키 세션 + crumb 토큰 취득"""
    global _yahoo_session, _yahoo_crumb
    if _yahoo_session and _yahoo_crumb:
        return _yahoo_session, _yahoo_crumb
    try:
        s = requests.Session()
        s.get("https://fc.yahoo.com", headers={"User-Agent": BROWSER_UA}, timeout=10)
        r = s.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers={"User-Agent": BROWSER_UA},
            timeout=10,
        )
        if r.status_code == 200 and r.text.strip():
            _yahoo_session = s
            _yahoo_crumb = r.text.strip()
            log.info(f"Yahoo crumb OK: {_yahoo_crumb[:8]}...")
        else:
            log.warning(f"Yahoo crumb failed: {r.status_code}")
    except Exception as e:
        log.warning(f"Yahoo crumb error: {e}")
    return _yahoo_session, _yahoo_crumb


def _fetch_pcr_yahoo() -> Optional[OptionsData]:
    """Yahoo v7 options API (crumb 인증)"""
    _yahoo_throttle()
    session, crumb = _get_yahoo_crumb()
    if not session or not crumb:
        return None
    try:
        resp = session.get(
            f"https://query1.finance.yahoo.com/v7/finance/options/{TICKER}",
            params={"crumb": crumb},
            headers={"User-Agent": BROWSER_UA},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"Yahoo options {resp.status_code}")
            return None
        options = resp.json()["optionChain"]["result"][0]["options"]
        put_oi = sum(p.get("openInterest", 0) for chain in options for p in chain.get("puts", []))
        call_oi = sum(c.get("openInterest", 0) for chain in options for c in chain.get("calls", []))
        if call_oi == 0:
            return None
        pcr = put_oi / call_oi
        od = OptionsData(pcr=round(pcr, 3), total_puts=put_oi, total_calls=call_oi)
        od.pcr_signal = "heavy_hedging" if pcr > 1.2 else "bullish" if pcr < 0.5 else "neutral"
        return od
    except Exception as e:
        log.warning(f"Yahoo PCR parse error: {e}")
        return None


def _fetch_pcr_cboe() -> Optional[OptionsData]:
    """CBOE delayed quotes fallback (인증 불필요)"""
    try:
        resp = safe_get(
            f"https://cdn.cboe.com/api/global/delayed_quotes/options/{TICKER}.json",
            headers={"User-Agent": BROWSER_UA},
        )
        if not resp:
            return None
        data = resp.json()
        options = data.get("data", {}).get("options", [])
        put_oi = sum(o.get("open_interest", 0) for o in options if o.get("option_type") == "P")
        call_oi = sum(o.get("open_interest", 0) for o in options if o.get("option_type") == "C")
        if call_oi == 0:
            return None
        pcr = put_oi / call_oi
        od = OptionsData(pcr=round(pcr, 3), total_puts=put_oi, total_calls=call_oi)
        od.pcr_signal = "heavy_hedging" if pcr > 1.2 else "bullish" if pcr < 0.5 else "neutral"
        log.info(f"PCR from CBOE: {pcr:.3f}")
        return od
    except Exception as e:
        log.warning(f"CBOE PCR error: {e}")
        return None


def fetch_options_pcr() -> Optional[OptionsData]:
    """Yahoo crumb 인증 시도 → 실패 시 CBOE fallback"""
    result = _fetch_pcr_yahoo()
    if result:
        return result
    log.info("Yahoo PCR failed, trying CBOE...")
    return _fetch_pcr_cboe()


# ─────────────────────────────────────────────
# 5. 공매도
# ─────────────────────────────────────────────
def fetch_short_interest() -> Optional[ShortInterestData]:
    now = datetime.now(UTC)
    # delta=1부터: 당일 파일은 장 마감 수 시간 후 생성되므로 전일부터 조회
    for delta in range(1, 8):
        d = now - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y%m%d")
        resp = safe_get(FINRA_SHORT_URL.format(date=date_str))
        if not resp or not resp.text.strip():
            continue
        try:
            for line in resp.text.strip().split("\n"):
                fields = line.split("|")
                if len(fields) >= 5 and fields[1].upper() == TICKER:
                    # float 형태로 올 수 있으므로 float 경유 후 int 변환
                    short_vol = int(float(fields[2]))
                    total_vol = int(float(fields[4]))
                    short_pct = (short_vol / total_vol * 100) if total_vol > 0 else 0
                    sid = ShortInterestData(
                        short_volume=short_vol, total_volume=total_vol,
                        short_pct=round(short_pct, 1), date=d.strftime("%Y-%m-%d"),
                    )
                    sid.signal = "high_short" if short_pct > 50 else "normal"
                    return sid
        except Exception as e:
            log.error(f"Short interest parse error ({date_str}): {e}")
    return None


# ─────────────────────────────────────────────
# 6. 발행사 중요 공시 + 내부자 거래
# ─────────────────────────────────────────────
MATERIAL_COMPANY_FORMS = {
    "8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A",
    "6-K", "6-K/A", "20-F", "20-F/A",
}


def fetch_company_filings(limit: int = 20, lookback_days: int = 21) -> list:
    """SEC submissions에서 발행사의 최근 중요 공시를 조회한다."""
    if not CIK_PADDED:
        _set_source_health("SEC 공시", "CIK 없음")
        return []
    resp = safe_get(
        f"https://data.sec.gov/submissions/CIK{CIK_PADDED}.json",
        headers=SEC_HEADERS,
    )
    if not resp:
        _set_source_health("SEC 공시", "실패")
        return []
    try:
        recent = resp.json().get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        cutoff = datetime.now(UTC).date() - timedelta(days=lookback_days)
        filings = []

        def value(key: str, idx: int) -> str:
            values = recent.get(key, [])
            return str(values[idx] or "").strip() if idx < len(values) else ""

        for idx, form_value in enumerate(forms):
            form = str(form_value).upper()
            if form not in MATERIAL_COMPANY_FORMS:
                continue
            filing_date = value("filingDate", idx)
            try:
                if filing_date and datetime.strptime(filing_date, "%Y-%m-%d").date() < cutoff:
                    continue
            except ValueError:
                pass
            accession = value("accessionNumber", idx)
            primary_doc = value("primaryDocument", idx).rsplit("/", 1)[-1]
            if not accession or not primary_doc:
                continue
            acc_clean = accession.replace("-", "")
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{CIK_SHORT}/"
                f"{acc_clean}/{primary_doc}"
            )
            filings.append(CompanyFiling(
                form=form,
                filing_date=filing_date,
                report_date=value("reportDate", idx),
                description=value("primaryDocDescription", idx) or f"{form} filing",
                accession=accession,
                url=url,
                hash=hashlib.md5(f"{accession}:{form}".encode()).hexdigest()[:12],
            ))
            if len(filings) >= limit:
                break
        _set_source_health("SEC 공시", "정상")
        return filings
    except Exception as e:
        log.warning(f"Company filings parse error: {e}")
        _set_source_health("SEC 공시", "실패")
        return []


def fetch_insider_trades() -> list:
    """
    data.sec.gov submissions JSON에서 Form 4 목록을 가져온 뒤
    각 filing의 원본 XML을 직접 열어 파싱. Atom feed는 fallback으로만 사용.
    """
    trades = []
    if not CIK_PADDED:
        log.info("Form 4 스킵: monitor_config.md에 cik 미설정")
        _set_source_health("SEC Form4", "CIK 없음")
        return trades
    try:
        candidates = _form4_candidates_from_submissions(limit=10)
        if candidates:
            log.info(f"Form 4: {len(candidates)} filings found via submissions JSON")
            for filing_date, xml_urls in candidates:
                xml_resp = None
                xml_url = ""
                for candidate_url in xml_urls:
                    log.info(f"Form 4 XML 요청: {candidate_url}")
                    xml_resp = safe_get(candidate_url, headers=SEC_LEGACY_HEADERS, retries=1)
                    if xml_resp:
                        xml_url = candidate_url
                        break
                if not xml_resp:
                    log.warning(f"Form 4 XML 응답 없음: {xml_urls[0] if xml_urls else ''}")
                    break
                try:
                    parsed = parse_form4_xml(xml_resp.text, filing_date, xml_url)
                    trades.extend(parsed)
                    log.info(f"Form 4 파싱 완료: {len(parsed)}건 — {xml_url}")
                except Exception as e:
                    log.warning(f"Form 4 parse error: {e}")
                time.sleep(0.3)
            if trades:
                _set_source_health("SEC Form4", "정상")
                return trades
            log.warning("SEC Form 4 원문 접근 불가 — Yahoo 내부자 거래 fallback")
            return _fetch_insider_trades_from_yahoo()

        log.warning("Form 4 submissions 후보 없음 — atom feed fallback")
        atom_trades = _fetch_insider_trades_from_atom()
        if atom_trades:
            _set_source_health("SEC Form4", "정상")
        return atom_trades or _fetch_insider_trades_from_yahoo()

    except Exception as e:
        log.error(f"Insider fetch error: {e}")
    return trades or _fetch_insider_trades_from_yahoo()


def _fetch_insider_trades_from_yahoo(limit: int = 30) -> list:
    """SEC Archives가 자동화 요청을 막을 때 Yahoo 거래 내역을 대체 사용."""
    try:
        import yfinance as yf

        data = yf.Ticker(TICKER).insider_transactions
        if data is None or data.empty:
            log.warning("Yahoo 내부자 거래 데이터 없음")
            _set_source_health("SEC Form4", "Yahoo 대체 데이터 없음")
            return []

        trades = []
        for _, row in data.head(limit).iterrows():
            text = str(row.get("Text") or "")
            lowered = text.lower()
            if "sale" in lowered:
                trade_type, txn_code = "Sale", "S"
            elif "purchase" in lowered or "buy" in lowered:
                trade_type, txn_code = "Purchase", "P"
            elif "award" in lowered or "grant" in lowered:
                trade_type, txn_code = "Award", "A"
            else:
                continue

            try:
                shares = int(float(row.get("Shares") or 0))
            except (TypeError, ValueError):
                shares = 0
            try:
                total_value = float(row.get("Value") or 0)
            except (TypeError, ValueError):
                total_value = 0.0
            if shares <= 0:
                continue

            price = total_value / shares if total_value > 0 else 0.0
            if price <= 0:
                price_match = re.search(r"price\s+([0-9,.]+)", text, re.IGNORECASE)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))

            date_value = row.get("Start Date")
            date = date_value.strftime("%Y-%m-%d") if hasattr(date_value, "strftime") else str(date_value or "")[:10]
            url = str(row.get("URL") or "")
            if not url:
                url = f"https://finance.yahoo.com/quote/{TICKER}/insider-transactions/"

            trades.append(InsiderTrade(
                filer=str(row.get("Insider") or "Unknown"),
                title=str(row.get("Position") or ""),
                trade_type=trade_type,
                txn_code=txn_code,
                shares=shares,
                price=round(price, 2),
                total_value=round(total_value, 2),
                date=date,
                url=url,
            ))

        log.info(f"Yahoo 내부자 거래 fallback: {len(trades)}건")
        _set_source_health("SEC Form4", "Yahoo 대체")
        return trades
    except Exception as e:
        log.warning(f"Yahoo 내부자 거래 fallback 실패: {e}")
        _set_source_health("SEC Form4", "실패")
        return []


def _recent_value(recent: dict, key: str, idx: int) -> str:
    values = recent.get(key, [])
    if idx >= len(values):
        return ""
    return str(values[idx] or "").strip()


def _form4_xml_urls_from_submission(accession: str, primary_doc: str) -> list:
    if not accession or not primary_doc:
        return []

    acc_clean = accession.replace("-", "")
    filing_cik = accession.split("-", 1)[0].lstrip("0") or CIK_SHORT
    primary_name = primary_doc.rsplit("/", 1)[-1]
    if not primary_name.lower().endswith(".xml"):
        return []

    ciks = [filing_cik]
    if CIK_SHORT and CIK_SHORT not in ciks:
        ciks.append(CIK_SHORT)

    return [
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{primary_name}"
        for cik in ciks
    ]


def _form4_candidates_from_submissions(limit: int = 10) -> list:
    resp = safe_get(f"https://data.sec.gov/submissions/CIK{CIK_PADDED}.json", headers=SEC_HEADERS)
    if not resp:
        return []

    data = resp.json()
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    candidates = []

    for idx, form in enumerate(forms):
        if str(form).upper() not in {"4", "4/A"}:
            continue

        accession = _recent_value(recent, "accessionNumber", idx)
        primary_doc = _recent_value(recent, "primaryDocument", idx)
        filing_date = _recent_value(recent, "filingDate", idx)
        xml_urls = _form4_xml_urls_from_submission(accession, primary_doc)
        if xml_urls:
            candidates.append((filing_date, xml_urls))
        if len(candidates) >= limit:
            break

    return candidates


def _fetch_insider_trades_from_atom() -> list:
    trades = []
    try:
        # atom feed: 설정 회사(CIK)가 issuer인 Form 4 목록
        resp = safe_get(
            "https://www.sec.gov/cgi-bin/browse-edgar",
            headers=SEC_LEGACY_HEADERS,
            params={
                "action": "getcompany",
                "CIK": CIK_PADDED,
                "type": "4",
                "dateb": "",
                "owner": "include",
                "count": "10",
                "output": "atom",
            },
        )
        if not resp:
            log.warning("Form 4 atom feed fetch failed")
            return trades

        root = ET.fromstring(resp.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall("atom:entry", ns)
        log.info(f"Form 4: {len(entries)} filings found via atom feed")

        for entry in entries[:10]:
            # EDGAR atom feed URL 추출 — 태그명이 버전마다 다름
            filing_href = ""

            # 방법 1: atom:filing-href 네임스페이스
            filing_href = entry.findtext("atom:filing-href", namespaces=ns) or ""

            # 방법 2: content 태그의 href 속성
            if not filing_href:
                content = entry.find("atom:content", ns)
                if content is not None:
                    filing_href = content.get("href", "")

            # 방법 3: entry 내 link 태그에서 Archives/edgar URL 찾기
            if not filing_href:
                for link in entry.findall("atom:link", ns):
                    href = link.get("href", "")
                    if "Archives/edgar" in href:
                        filing_href = href
                        break

            # 방법 4: atom:id (일부 버전에서 URL 포함)
            if not filing_href:
                entry_id = entry.findtext("atom:id", namespaces=ns) or ""
                if "Archives/edgar" in entry_id:
                    filing_href = entry_id

            filing_date = (entry.findtext("atom:updated", namespaces=ns) or "")[:10]

            if not filing_href:
                # 디버그용: entry 태그 목록 출력
                tags = [child.tag for child in entry]
                log.warning(f"Form 4 filing-href 없음 — entry 태그: {tags}")
                continue

            log.info(f"Form 4 index 요청: {filing_href}")
            idx_resp = safe_get(filing_href, headers=SEC_LEGACY_HEADERS, retries=1)
            if not idx_resp:
                log.warning(f"Form 4 index 응답 없음: {filing_href}")
                continue

            xml_url = _find_form4_xml_url(idx_resp.text, filing_href)
            if not xml_url:
                log.warning(f"Form 4 XML 링크 미발견 (index HTML 길이={len(idx_resp.text)}): {filing_href}")
                continue

            log.info(f"Form 4 XML 요청: {xml_url}")
            xml_resp = safe_get(xml_url, headers=SEC_LEGACY_HEADERS, retries=1)
            if xml_resp:
                try:
                    parsed = parse_form4_xml(xml_resp.text, filing_date, xml_url)
                    trades.extend(parsed)
                    log.info(f"Form 4 파싱 완료: {len(parsed)}건 — {xml_url}")
                except Exception as e:
                    log.warning(f"Form 4 parse error: {e}")
            else:
                log.warning(f"Form 4 XML 응답 없음: {xml_url}")

            time.sleep(0.3)

    except Exception as e:
        log.error(f"Insider atom fallback error: {e}")
    return trades


def _find_form4_xml_url(index_html: str, index_url: str) -> str:
    """
    Form 4 index HTML에서 원본 XML 파일 URL 추출.
    예: .../0002049077-26-000009-index.htm 페이지에서
        wk-form4_xxxx.xml 링크 찾기
    """
    import re
    # base URL: index URL에서 파일명 제거
    base = index_url.rsplit("/", 1)[0] + "/"

    # href에서 .xml 파일 찾기 (xsl 렌더링 경로 제외)
    for href in re.findall(r'href=["\']([^"\']+)["\']', index_html):
        name = href.split("/")[-1]
        # 파일명과 경로 모두에서 xsl 렌더링 경로 제외
        if (name.endswith(".xml")
                and "xsl" not in href.lower()       # 경로에 xslF345 등 포함 여부
                and not name.startswith("R")
                and "Financial" not in name):
            # 절대 경로면 그대로, 상대 경로면 base 붙이기
            if href.startswith("http"):
                return href
            return "https://www.sec.gov" + href if href.startswith("/") else base + name
    return ""


def parse_form4_xml(xml_text: str, filing_date: str, url: str) -> list:
    trades = []
    try:
        xml_clean = xml_text
        for ns in ['xmlns="http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"',
                   'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"']:
            xml_clean = xml_clean.replace(ns, "")
        root = ET.fromstring(xml_clean)

        reporter = root.find(".//reportingOwner/reportingOwnerId")
        if reporter is None:
            reporter = root.find(".//reportingOwnerId")
        filer_name = reporter.findtext("rptOwnerName", "Unknown") if reporter is not None else "Unknown"

        rel = root.find(".//reportingOwner/reportingOwnerRelationship")
        if rel is None:
            rel = root.find(".//reportingOwnerRelationship")

        filer_title = ""
        if rel is not None:
            filer_title = rel.findtext("officerTitle", "").strip()
            # officerTitle 없으면 역할 필드로 fallback
            if not filer_title:
                is_director  = rel.findtext("isDirector", "0").strip()
                is_officer   = rel.findtext("isOfficer", "0").strip()
                is_10pct     = rel.findtext("isTenPercentOwner", "0").strip()
                if is_director == "1":
                    filer_title = "Director"
                elif is_officer == "1":
                    filer_title = "Officer"
                elif is_10pct == "1":
                    filer_title = "10% Owner"

        for txn in root.findall(".//nonDerivativeTransaction"):
            t = _parse_transaction(txn, filer_name, filer_title, filing_date, url)
            if t:
                trades.append(t)
        for txn in root.findall(".//derivativeTransaction"):
            t = _parse_transaction(txn, filer_name, filer_title, filing_date, url)
            if t:
                trades.append(t)
    except ET.ParseError:
        log.warning("Form 4 XML parse error")
    except Exception as e:
        log.warning(f"Form 4 detail error: {e}")
    return trades


def _parse_transaction(txn, filer_name, filer_title, filing_date, url):
    try:
        coding = txn.find("transactionCoding")
        txn_code = ""
        if coding is not None:
            txn_code_e = coding.find("transactionCode")
            txn_code = txn_code_e.text.strip() if txn_code_e is not None and txn_code_e.text else ""

        # 제외 코드: 주주 입장에서 의미 없는 비시장 거래
        # C=전환, J=기타, G=증여, W=상속, Z=신탁
        SKIP_CODES = {"C", "J", "G", "W", "Z"}
        if txn_code in SKIP_CODES:
            log.debug(f"Form 4 스킵 (code={txn_code}): {filer_name}")
            return None

        amounts = txn.find("transactionAmounts")
        if amounts is None:
            return None

        shares_e = amounts.find("transactionShares/value")
        price_e  = amounts.find("transactionPricePerShare/value")
        code_e   = amounts.find("transactionAcquiredDisposedCode/value")

        shares = float(shares_e.text) if shares_e is not None and shares_e.text else 0
        price  = float(price_e.text)  if price_e  is not None and price_e.text  else 0
        acq    = code_e.text.strip()  if code_e   is not None and code_e.text   else ""

        if shares == 0:
            return None

        # ── 거래 유형 분류 ──────────────────────────────────
        # P = 장내 매수 (Open Market Purchase)  → 강한 강세 시그널
        # S = 장내 매도 (Open Market Sale)      → 강한 약세 시그널
        # A = RSU 귀속/스톡옵션 부여 (Award)    → 보상, 시장 신호 아님
        # D = 처분 (세금 원천징수 등 비시장 거래 포함) → 방향성 판단 금지
        if txn_code == "P":
            trade_type = "Purchase"
        elif txn_code == "S":
            trade_type = "Sale"
        elif txn_code == "A":
            trade_type = "Award"     # RSU 귀속 / 스톡옵션 부여
        elif txn_code == "D" or acq == "D":
            trade_type = "Disposition"
        else:
            return None              # 분류 불가 → 스킵

        return InsiderTrade(
            filer=filer_name, title=filer_title, trade_type=trade_type,
            txn_code=txn_code,
            shares=int(shares), price=round(price, 2),
            total_value=round(shares * price, 2),
            date=filing_date, url=url,
        )
    except Exception as e:
        log.debug(f"_parse_transaction error: {e}")
        return None


# ─────────────────────────────────────────────
# 7. 13F (BUG 3 FIX: API URL + infoTable 파싱)
# ─────────────────────────────────────────────
def fetch_13f_filings() -> list:
    """
    BUG 3 FIX:
    - EDGAR search API URL 수정 (파라미터 형식)
    - 실제 13F XML에서 설정 종목 보유 주식 수 / 평가금액 파싱
    """
    filings = []
    try:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
        start_date = (datetime.now(UTC) - timedelta(days=120)).strftime("%Y-%m-%d")

        # 수정된 EDGAR full-text search URL
        search_url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{COMPANY_NAME or TICKER}"',
            "forms": "13F-HR",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
        }
        resp = safe_get(search_url, headers=SEC_HEADERS, params=params)
        if not resp:
            # 대안: 티커로 검색
            params["q"] = f'"{TICKER}"'
            resp = safe_get(search_url, headers=SEC_HEADERS, params=params)
        if not resp:
            log.warning("13F EDGAR search failed")
            return filings

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        log.info(f"13F: {len(hits)} filings found")

        for hit in hits[:10]:
            source = hit.get("_source", {})
            display_names = source.get("display_names", [])
            entity = display_names[0] if display_names else source.get("entity_name", "Unknown")
            filing_date = source.get("file_date", "")
            accession_raw = source.get("accession_no", "")
            if not accession_raw:
                continue

            # infoTable XML 파싱으로 설정 종목 포지션 추출
            acc_clean = accession_raw.replace("-", "")
            entity_cik = source.get("entity_id", "")

            shares, value_usd, change_type = _parse_13f_position(entity_cik, acc_clean)

            filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={entity_cik}&type=13F-HR&dateb=&owner=include&count=5"

            filings.append(Filing13F(
                institution=entity if isinstance(entity, str) else str(entity),
                shares=shares,
                value_usd=value_usd,
                change_type=change_type,
                filing_date=filing_date,
                url=filing_url,
            ))
            time.sleep(0.3)

    except Exception as e:
        log.error(f"13F fetch error: {e}")
    return filings


def _parse_13f_position(entity_cik: str, acc_clean: str) -> tuple:
    """
    13F infoTable XML에서 설정 종목 포지션(주식 수, 평가금액) 추출
    Returns: (shares, value_usd, change_type)
    """
    if not entity_cik:
        return 0, 0.0, ""

    cik_num = entity_cik.lstrip("0") or "0"
    idx_url = f"https://data.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/index.json"
    idx_resp = safe_get(idx_url, headers=SEC_HEADERS, retries=1)
    if not idx_resp:
        return 0, 0.0, ""

    try:
        items = idx_resp.json().get("directory", {}).get("item", [])
        # infoTable XML 찾기
        info_url = None
        for item in items:
            name = item.get("name", "").lower()
            if "infotable" in name and name.endswith(".xml"):
                info_url = f"https://data.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{item['name']}"
                break
        if not info_url:
            # 대안: form 파일에서 XML 직접 찾기
            for item in items:
                name = item.get("name", "")
                if name.endswith(".xml") and "xsl" not in name.lower():
                    info_url = f"https://data.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{name}"
                    break

        if not info_url:
            return 0, 0.0, ""

        xml_resp = safe_get(info_url, headers=SEC_HEADERS, retries=1)
        if not xml_resp:
            return 0, 0.0, ""

        return _extract_hood_from_infotable(xml_resp.text)

    except Exception as e:
        log.debug(f"13F infoTable parse error: {e}")
        return 0, 0.0, ""


def _extract_hood_from_infotable(xml_text: str) -> tuple:
    """infoTable XML에서 설정 종목 항목을 찾아 주식 수와 평가금액 추출"""
    try:
        # 네임스페이스 제거
        xml_clean = xml_text
        import re
        xml_clean = re.sub(r'\s+xmlns[^"]*"[^"]*"', "", xml_clean)
        xml_clean = re.sub(r'\s+xmlns[^=]*=\S+', "", xml_clean)

        root = ET.fromstring(xml_clean)

        # infoTable 항목 순회
        for info in root.iter("infoTable"):
            name_elem = info.find("nameOfIssuer")
            if name_elem is None:
                continue
            name = name_elem.text or ""
            # 티커 또는 회사명 키워드 포함 여부 확인
            name_upper = name.upper()
            if not any(keyword in name_upper for keyword in CONFIG.issuer_keywords):
                continue

            shares_elem = info.find("shrsOrPrnAmt/sshPrnamt") or info.find("sshPrnamt")
            value_elem = info.find("value")
            put_call_elem = info.find("putCall")

            # 옵션 제외 (주식만)
            if put_call_elem is not None and put_call_elem.text:
                continue

            shares = int(shares_elem.text.replace(",", "")) if shares_elem is not None and shares_elem.text else 0
            value = float(value_elem.text.replace(",", "")) * 1000 if value_elem is not None and value_elem.text else 0.0

            return shares, value, "REPORTED"

    except Exception as e:
        log.debug(f"infoTable XML parse: {e}")

    return 0, 0.0, ""


# ─────────────────────────────────────────────
# 8. DCA 시그널 (BUG 4 FIX: 결론 명확화, 가격 숫자 제거)
# ─────────────────────────────────────────────
def _is_recent(date_str: str, days: int) -> bool:
    try:
        return (datetime.now() - datetime.strptime(date_str, "%Y-%m-%d")).days <= days
    except Exception:
        return False


# ─────────────────────────────────────────────
# Slack 포맷터 (Section + Context 구조)
# ─────────────────────────────────────────────
def _ctx(text: str) -> dict:
    """Context 블록 헬퍼 — 작은 글씨 보조 정보"""
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _sec(text: str, fields: list = None) -> dict:
    """Section 블록 헬퍼. fields 있으면 2열 레이아웃"""
    block = {"type": "section", "text": {"type": "mrkdwn", "text": text}}
    if fields:
        block["fields"] = [{"type": "mrkdwn", "text": f} for f in fields]
    return block


def make_volume_activity(
    current_volume: int,
    average_volume: int,
    *,
    window: int = VOLUME_LOOKBACK_DAYS,
    threshold: float = VOLUME_EXPLOSION_RATIO,
) -> Optional[VolumeActivity]:
    """현재 거래량과 과거 평균을 비교해 일관된 거래량 신호를 만든다."""
    if current_volume <= 0 or average_volume <= 0:
        return None
    ratio = current_volume / average_volume
    return VolumeActivity(
        current_volume=current_volume,
        average_volume=average_volume,
        ratio=ratio,
        window=window,
        exploded=ratio >= threshold,
    )


def calculate_volume_activity(
    volumes: list,
    *,
    window: int = VOLUME_LOOKBACK_DAYS,
    threshold: float = VOLUME_EXPLOSION_RATIO,
) -> Optional[VolumeActivity]:
    """마지막 거래일을 직전 N개 거래일 평균과 비교한다."""
    if len(volumes) < window + 1:
        return None
    current_volume = int(volumes[-1] or 0)
    baseline = [int(volume) for volume in volumes[-window - 1:-1] if volume]
    if current_volume <= 0 or len(baseline) != window:
        return None
    average_volume = int(sum(baseline) / window)
    return make_volume_activity(
        current_volume,
        average_volume,
        window=window,
        threshold=threshold,
    )


def format_volume_activity_block(
    activity: Optional[VolumeActivity],
    *,
    finalized: bool,
) -> list:
    if not activity:
        return []
    if activity.exploded:
        headline = "🔥 거래량 터짐"
    elif finalized:
        headline = "⚪ 거래량 안 터짐"
    else:
        headline = "⏳ 장중 거래량 기준 미달"
    volume_label = "당일 거래량" if finalized else "당일 누적 거래량"
    return [
        _sec(
            f"*{headline}*\n"
            f"{volume_label} *{activity.current_volume:,}주*"
        ),
        _ctx(
            f"{activity.window}일 평균 {activity.average_volume:,}주 | "
            f"평균 대비 *{activity.ratio:.2f}x* | "
            f"터짐 기준 {VOLUME_EXPLOSION_RATIO:.2f}x"
        ),
    ]


def _thesis_decision(news: list, filings: list, insiders: Optional[list] = None) -> tuple[str, str]:
    impacts = {item.get("thesis_impact", "neutral") for item in news}
    if "damage" in impacts:
        return "훼손 가능성", "중단 검토"
    has_open_market_sale = any(
        trade.trade_type == "Sale" for trade in (insiders or [])
    )
    if "risk" in impacts or filings or has_open_market_sale:
        return "확인 필요", "점검 필요"
    if "strengthen" in impacts:
        return "강화 근거 확인", "기존 계획 유지"
    return "변화 없음", "기존 계획 유지"


def build_decision_summary_blocks(
    *,
    price: Optional[PriceData],
    benchmark_pct: Optional[float],
    news: list,
    filings: list,
    insiders: Optional[list] = None,
    volume_activity: Optional[VolumeActivity] = None,
    source_health: Optional[dict] = None,
) -> list:
    """가격 숫자 없이 하루의 핵심 의사결정을 먼저 보여준다."""
    direction = _direction_label(price.change_pct if price else None)
    relative = _relative_label(price.change_pct if price else None, benchmark_pct)
    thesis, dca = _thesis_decision(news, filings, insiders)
    material_news = [item for item in news if not item.get("skip") and item.get("summary")]
    directional_insiders = [
        trade for trade in (insiders or [])
        if trade.trade_type in {"Purchase", "Sale"}
    ]
    event_count = (
        len(material_news)
        + len(filings)
        + len(directional_insiders)
        + (1 if volume_activity and volume_activity.exploded else 0)
    )
    event_text = f"중요 변화 {event_count}건" if event_count else "새로운 중요 사건 없음"
    health = source_health if source_health is not None else SOURCE_HEALTH
    has_source_failure = any(
        any(token in status for token in ("실패", "키 없음", "확인 불가"))
        for status in health.values()
    )
    if has_source_failure and not event_count:
        event_text = "판정 보류 (일부 데이터 실패)"
        thesis, dca = "확인 불가", "점검 필요"
    health_text = " · ".join(f"{name} {status}" for name, status in health.items()) or "수집 상태 확인 불가"
    icon = "🔴" if dca == "중단 검토" else "🟡" if dca == "점검 필요" else "🟢"
    return [
        _sec(
            f"*{icon} 오늘의 판단*\n"
            f"시장 방향: *{direction}*\n"
            f"상대 성과: *{relative}*\n"
            f"핵심 변화: *{event_text}*\n"
            f"투자 논지: *{thesis}*\n"
            f"DCA 계획: *{dca}*"
        ),
        _ctx(f"데이터 상태: {health_text}"),
        {"type": "divider"},
    ]


def format_technicals_block(ts: TechnicalSignals) -> list:
    if ts.rsi_14 <= 30:
        rsi_line = f"🟢 *RSI {ts.rsi_14}* — 기술적 과매도"
    elif ts.rsi_14 <= 40:
        rsi_line = f"🟡 *RSI {ts.rsi_14}* — 약세 구간"
    elif ts.rsi_14 >= 70:
        rsi_line = f"🔴 *RSI {ts.rsi_14}* — 기술적 과열"
    else:
        rsi_line = f"⚪ *RSI {ts.rsi_14}* — 중립"

    macd_line = ""
    if ts.macd_alert == "bullish_cross":
        macd_line = "  🟢 MACD 골든크로스"
    elif ts.macd_alert == "bearish_cross":
        macd_line = "  🔴 MACD 데드크로스"

    return [
        _sec(f"*📊 기술 지표*\n{rsi_line}{macd_line}"),
        _ctx(f"MACD {ts.macd_line:+.4f} | Signal {ts.macd_signal:+.4f} | Hist {ts.macd_histogram:+.4f}"),
    ]


def format_options_block(od: OptionsData) -> list:
    sig = {
        "heavy_hedging": "🟡 과도한 풋 헤징",
        "bullish": "🟢 콜 우세",
        "neutral": "⚪ 중립",
    }
    return [
        _sec(f"*📈 옵션 시장*  PCR: *{od.pcr:.3f}* — {sig.get(od.pcr_signal, '')}"),
        _ctx(f"풋 OI {od.total_puts:,} | 콜 OI {od.total_calls:,}"),
    ]


def format_short_block(si: ShortInterestData) -> list:
    return [
        _sec(f"*🩳 FINRA 일일 공매도 체결 비중*  *{si.short_pct:.1f}%*"),
        _ctx(
            f"기준일 {si.date} | 공매도 체결 {si.short_volume:,} / 보고 거래량 {si.total_volume:,} | "
            "미결제 공매도 잔고(short interest)가 아닙니다."
        ),
    ]


def format_insider_block(trades: list) -> list:
    if not trades:
        return []

    # 거래 유형별 표시 맵
    TYPE_MAP = {
        "Purchase": ("🟢", "장내 매수"),
        "Sale":     ("🔴", "장내 매도"),
        "Award":    ("🔵", "RSU 귀속"),
        "Disposition": ("⚪", "비시장 처분 가능"),
    }
    # txn_code 보조 레이블 (Purchase/Sale 내에서 세분화)
    CODE_LABEL = {
        "P": "장내 매수",
        "S": "장내 매도",
        "A": "RSU 귀속",
        "D": "비시장 처분 가능",
    }

    lines = []
    for t in trades[:6]:
        emoji, type_label = TYPE_MAP.get(t.trade_type, ("⚪", t.trade_type))
        # txn_code로 세분화된 레이블 우선 사용
        if t.txn_code in CODE_LABEL:
            type_label = CODE_LABEL[t.txn_code]

        # 직함 표시 (없으면 생략)
        title_str = f" _{t.title}_" if t.title else ""

        # 규모 산정
        if t.total_value >= 1_000_000:
            scale = "대규모"
        elif t.total_value >= 100_000:
            scale = "중규모"
        elif t.total_value > 0:
            scale = "소규모"
        else:
            scale = "대규모" if t.shares >= 50_000 else "중규모" if t.shares >= 5_000 else "소규모"

        # 가격: RSU 귀속은 가격 없음이 정상이므로 표시 생략
        source_link = f"  <{t.url}|원문>" if t.url else ""

        lines.append(
            f"{emoji} *{t.filer}*{title_str}\n"
            f"   {type_label}  {t.shares:,}주  {scale}  _{t.date}_{source_link}"
        )

    return [
        _sec("*🕴 내부자 거래*\n" + "\n".join(lines)),
        _ctx("🟢 장내 매수  🔴 장내 매도  🔵 보상  ⚪ 비시장 처분 가능(방향성 판단 제외)"),
    ]


def material_insider_trades(
    trades: list,
    lookback_days: int = 21,
    as_of=None,
) -> list:
    """최근 발생한 장내 거래 중 즉시 판단 가치가 있는 항목만 반환한다."""
    as_of = as_of or datetime.now(NY_TZ).date()
    cutoff = as_of - timedelta(days=lookback_days)
    material = []
    for trade in trades:
        try:
            trade_date = datetime.strptime(trade.date, "%Y-%m-%d").date()
            if trade_date < cutoff or trade_date > as_of:
                continue
        except (TypeError, ValueError):
            pass
        if trade.trade_type == "Purchase":
            material.append(trade)
        elif trade.trade_type == "Sale" and (
            trade.total_value >= 100_000 or trade.shares >= 1_000
        ):
            material.append(trade)
    return material


def format_company_filings_block(filings: list) -> list:
    if not filings:
        return []
    lines = []
    for filing in filings[:6]:
        report = f" · 보고기간 {filing.report_date}" if filing.report_date else ""
        link = f"<{filing.url}|SEC 원문>" if filing.url else "SEC 원문 없음"
        lines.append(
            f"📄 *{filing.form}* — {filing.description}{report}\n"
            f"   _제출 {filing.filing_date}_ · {link} · 내용 확인 필요"
        )
    return [
        _sec("*🏛 발행사 중요 SEC 공시*\n" + "\n".join(lines)),
        _ctx("공시 제출 사실과 투자 논지 해석을 분리합니다. 원문 확인 전 방향을 단정하지 않습니다."),
    ]


def format_13f_block(filings: list) -> list:
    if not filings:
        return []
    lines = []
    for f in filings[:6]:
        detail = ""
        if f.shares > 0:
            val_str = f"${f.value_usd/1_000_000:.1f}M" if f.value_usd >= 1_000_000 else f"${f.value_usd:,.0f}"
            detail = f"  {f.shares:,}주 / {val_str}"
        change = {
            "BASELINE": "기준 설정",
            "NEW": "신규",
            "INCREASE": "증가",
            "DECREASE": "감소",
            "EXIT": "청산",
            "UNCHANGED": "유지",
        }.get(f.change_type, "보고")
        link = f"  <{f.url}|원문>" if f.url else ""
        lines.append(f"📋 *{f.institution}* · {change}{detail}  _{f.filing_date}_{link}")
    return [
        _sec("*🏛 13F 기관 포지션*\n" + "\n".join(lines)),
    ]


def format_news_block(news: list) -> list:
    relevant = [n for n in news if not n.get("skip") and n.get("summary")]
    if not relevant:
        return []
    blocks = []
    impact_labels = {
        "strengthen": "논지 강화",
        "neutral": "논지 중립",
        "risk": "위험 증가",
        "damage": "논지 훼손",
    }
    for n in relevant[:5]:
        tag = "🟢" if n.get("sentiment") == "positive" else "🔴" if n.get("sentiment") == "negative" else "⚪"
        blocks.append(_sec(f"*📰 {tag} {n['summary']}*"))
        if n.get("translation"):
            blocks.append(_ctx(f"사실: {n['translation']}"))
        impact = impact_labels.get(n.get("thesis_impact", "neutral"), "논지 중립")
        reason = n.get("impact_reason", "")
        confidence = n.get("confidence", "medium")
        source = n.get("source", "원문")
        link = f"<{n.get('link')}|{source} 원문>" if n.get("link") else source
        impact_line = f"해석: *{impact}*"
        if reason:
            impact_line += f" — {reason}"
        blocks.append(_ctx(f"{impact_line} | 확신도 {confidence} | {link}"))
    return blocks




def format_volume_profile_block(vp: VolumeProfile) -> list:
    poc_emoji = "🔴" if vp.poc_signal == "resistance" else "🟢"
    poc_desc = "매물대 상단 (저항)" if vp.poc_signal == "resistance" else "지지선 확보"
    whale = "  🐋 *Whale Activity Detected*" if vp.whale_detected else ""
    return [
        _sec(f"*📊 수급 구조 (30분 POC)*  {poc_emoji} *{poc_desc}*{whale}"),
        _ctx(f"30분 거래량 {vp.vol_30m:,} | 동시간대 5일평균 {vp.vol_avg_30m:,} | *{vp.vol_ratio:.1f}x*"),
    ]


def format_safety_margin_block(sm: SafetyMargin) -> list:
    blocks = []

    # ── 볼린저 밴드 ──
    bb_map = {
        "extreme_oversold": "🟢 *Extreme Oversold* — 밴드 하단 이탈, 통계적 반등 구간",
        "oversold":         "🟡 *밴드 하단 근접* — 과매도 경계",
        "overbought":       "🔴 *밴드 상단 돌파* — 과매수",
        "normal":           "⚪ *밴드 내 정상*",
    }
    mom_map = {
        "accelerating":  "📉 하락 가속 — 추가 하락 주의",
        "decelerating":  "📈 하락 둔화 — 저점 탐색 중",
        "stable":        "➡️ 모멘텀 안정",
    }
    blocks.append(_sec(
        f"*🛡 안전 마진*  {bb_map.get(sm.bb_signal, '')}  |  {mom_map.get(sm.momentum_signal, '')}"
    ))
    blocks.append(_ctx(
        "가격 레벨과 정확한 일일 등락률은 표시하지 않습니다."
    ))

    # ── 베타 초과 이탈 ──
    if sm.beta_excess_pct != 0:
        if sm.beta_excess_pct <= -3:
            beta_line = "🟢 *벤치마크 기대보다 과도한 하락* — 원인 확인 필요"
        elif sm.beta_excess_pct <= -1:
            beta_line = "🟡 *벤치마크 기대보다 소폭 약세*"
        elif sm.beta_excess_pct >= 3:
            beta_line = "🟢 *벤치마크 기대보다 강한 상승*"
        else:
            beta_line = "⚪ *벤치마크 기대 범위 내*"
        blocks.append(_ctx(f"{BETA_BENCHMARK} 기준 상대 흐름: {beta_line}"))

    # ── 피어 분기 경고 (Divergence Warning만 유지, 일반 피어 수치는 상대강도 블록에서 표시) ──
    if sm.divergence_warning:
        blocks.append(_sec(
            f"⚠️ *Divergence Warning*\n"
            f"{TICKER} 하락 가속 중인데 피어 그룹 반등 중 — 개별 악재 가능성"
        ))
        peer_text = "/".join(sm.peer_changes) or "피어 그룹"
        blocks.append(_ctx(f"{peer_text}와 다른 방향이 감지됨. 개별 원인을 확인하세요."))

    return blocks



# ─────────────────────────────────────────────
# Slack 전송
# ─────────────────────────────────────────────
def _footer() -> list:
    """메시지 끝 구분선 + 타임스탬프"""
    kst = datetime.now(KST).strftime("%m/%d %H:%M KST")
    return [
        {"type": "divider"},
        _ctx(f"🤖 {TICKER} Monitor  |  {kst}"),
    ]


def send_slack(blocks: list, text: str = ""):
    text = text or f"{TICKER} Monitor"
    if not SLACK_WEBHOOK:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            raise RuntimeError("SLACK_WEBHOOK_URL not set")
        log.warning("SLACK_WEBHOOK_URL not set")
        for b in blocks:
            if isinstance(b.get("text"), dict):
                print(b["text"].get("text", ""))
        return
    for i in range(0, len(blocks), 40):
        chunk = blocks[i:i + 40]
        resp = requests.post(SLACK_WEBHOOK, json={"text": text, "blocks": chunk}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"Slack error: {resp.status_code} {resp.text[:200]}")
        log.info(f"Slack sent OK ({i + 1}-{i + len(chunk)}/{len(blocks)})")


# ─────────────────────────────────────────────
# 실행 모드
# ─────────────────────────────────────────────
def run_normal():
    """장중 모드: 새 사실과 의미 있는 이상 움직임만 알린다."""
    log.info("=== NORMAL ===")
    SOURCE_HEALTH.clear()
    state = load_state()
    ws = load_weekly_state()
    blocks = []
    today = datetime.now(NY_TZ).strftime("%Y-%m-%d")

    if state.get("price_alert_date") != today:
        state["price_alert_max_pct"] = 0
        state["price_alert_direction"] = ""
        state["price_alert_date"] = today

    price = fetch_price()
    _set_source_health("Yahoo 시세", "정상" if price else "실패")
    if price and price.prev_close > 0:
        log.info(f"가격: {price.change_pct:+.2f}% | market_state={price.market_state}")

        # 완전 마감(CLOSED)일 때만 가격 알림 스킵 — PRE/POST는 실제 거래 있으므로 허용
        if price.market_state == "CLOSED":
            log.info("CLOSED 상태 — 가격 알림 스킵")
        else:
            abs_pct = abs(price.change_pct)
            direction = "up" if price.change_pct > 0 else "down"
            prev_max = state.get("price_alert_max_pct", 0)
            prev_dir = state.get("price_alert_direction", "")

            should_alert = (
                abs_pct >= 4 and (
                    prev_max == 0
                    or (direction == prev_dir and abs_pct >= prev_max + 1)
                    or (direction != prev_dir and abs_pct >= 4)
                )
            )
            if should_alert:
                emoji = "🚀" if direction == "up" else "💥"
                label = _direction_label(price.change_pct)
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                    f"{emoji} *{DISPLAY_TICKER} 큰 폭 {label} 감지*\n정확한 가격과 일일 등락률은 표시하지 않습니다."}})

                benchmark_pct = _fetch_ticker_change(BETA_BENCHMARK)
                if benchmark_pct is not None:
                    blocks.extend(format_beta_block({
                        "actual_pct": price.change_pct,
                        "qqq_pct": benchmark_pct,
                    }))

                # Volume Profile
                vp = analyze_volume_profile(price.current)
                if vp:
                    blocks.extend(format_volume_profile_block(vp))

                state["price_alert_max_pct"] = abs_pct if direction != prev_dir else max(prev_max, abs_pct)
                state["price_alert_direction"] = direction
                ws.setdefault("alerts_fired", []).append(f"큰 폭 {label}")

            volume_activity = make_volume_activity(price.volume, price.vol_avg_20d)
            if (
                volume_activity
                and volume_activity.exploded
                and state.get("volume_alert_date") != today
            ):
                blocks.extend(format_volume_activity_block(volume_activity, finalized=False))
                state["volume_alert_date"] = today
                ws.setdefault("alerts_fired", []).append(
                    f"거래량 터짐 ({VOLUME_LOOKBACK_DAYS}일 평균 대비 "
                    f"{volume_activity.ratio:.2f}배)"
                )

    closes = fetch_price_history(60)
    technicals = TechnicalSignals()
    if closes:
        state["price_history"] = closes[-60:]
        technicals = get_technical_signals(closes)
        if technicals.rsi_alert or technicals.macd_alert:
            blocks.extend(format_technicals_block(technicals))
        ws.setdefault("rsi_readings", []).append(technicals.rsi_14)

    company_filings = fetch_company_filings()
    new_filings = [filing for filing in company_filings
                   if filing.hash not in state.get("last_company_filing_hashes", [])]
    if new_filings:
        blocks.extend(format_company_filings_block(new_filings))
        remember_company_filings(state, new_filings)
        for filing in new_filings:
            ws.setdefault("company_filings", []).append(
                f"{filing.form}: {filing.description} ({filing.filing_date})"
            )
            ws.setdefault("thesis_impacts", []).append("risk")

    news = fetch_news()
    new_news = [n for n in news if n["hash"] not in state.get("last_news_hashes", [])]
    if new_news:
        new_news = translate_news(new_news)
        blocks.extend(format_news_block(new_news))
        remember_analyzed_news(state, new_news)
        for n in new_news:
            if not n.get("skip") and n.get("summary"):
                ws.setdefault("news_headlines", []).append(n["summary"])
                ws.setdefault("thesis_impacts", []).append(n.get("thesis_impact", "neutral"))

    insider_trades = fetch_insider_trades()
    new_insider_candidates = [t for t in insider_trades
                              if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                              not in state.get("last_insider_hashes", [])]
    new_insiders = material_insider_trades(new_insider_candidates)
    if new_insider_candidates:
        state["last_insider_hashes"] = [
            hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
            for t in insider_trades[:30]
        ]
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))
        for t in new_insiders:
            ws.setdefault("insider_trades", []).append(
                f"{t.trade_type}: {t.filer} {t.shares:,}주 "
                + ("대규모" if t.total_value >= 1_000_000 else "중규모" if t.total_value >= 100_000 else "소규모")
            )

    if blocks:
        blocks.insert(0, {"type": "header", "text": {"type": "plain_text",
            "text": f"🔔 {DISPLAY_TICKER} 중요 변화 — {datetime.now(KST).strftime('%m/%d %H:%M KST')}"}})
        blocks.insert(1, _ctx(
            "데이터 상태: "
            + " · ".join(f"{name} {status}" for name, status in SOURCE_HEALTH.items())
        ))
        blocks.extend(_footer())
        send_slack(blocks)
    else:
        log.info("No alerts — quiet")

    save_state(state)
    save_weekly_state(ws)


def run_close():
    """
    장 마감 모드: 핵심 판단을 먼저 보여주고 기존의 유용한 상세를 보존한다.
    """
    log.info("=== CLOSE ===")
    SOURCE_HEALTH.clear()
    state = load_state()
    ws = load_weekly_state()
    blocks = []
    benchmark_pct = None
    volume_activity = None
    new_filings = []
    new_news = []

    price = fetch_price(realtime=False)
    today_market = datetime.now(NY_TZ).strftime("%Y-%m-%d")
    if price and price.market_date and price.market_date != today_market:
        log.info(
            f"No fresh market bar ({price.market_date} != {today_market}) — close alert skipped"
        )
        return
    _set_source_health("Yahoo 시세", "정상" if price else "실패")
    if price and price.prev_close > 0:
        # 로깅: 등락률 항상 기록
        log.info(f"종가 등락: {price.change_pct:+.2f}% (prev_close={price.prev_close}, current={price.current})")

        # 반복 아침 알림은 제품 계약에서 제외한다.
        state["pending_morning_alert"] = None

        state["price_alert_max_pct"] = 0
        state["price_alert_direction"] = ""

    # OHLCV fetch (closes + highs/lows/volumes, 210일 — EMA200 + DCA 기술지표용)
    ohlcv = fetch_ohlcv(days=210)
    closes = ohlcv.get("closes") or fetch_price_history(60)  # 빈 리스트도 fallback
    volume_activity = calculate_volume_activity(ohlcv.get("volumes", []))
    if not volume_activity and price:
        volume_activity = make_volume_activity(price.volume, price.vol_avg_20d)
    blocks.extend(format_volume_activity_block(volume_activity, finalized=True))
    if (
        volume_activity
        and volume_activity.exploded
        and state.get("volume_alert_date") != today_market
    ):
        state["volume_alert_date"] = today_market
        ws.setdefault("alerts_fired", []).append(
            f"거래량 터짐 ({VOLUME_LOOKBACK_DAYS}일 평균 대비 "
            f"{volume_activity.ratio:.2f}배)"
        )
    weekly_ohlcv = fetch_weekly_ohlcv(weeks=40)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    blocks.extend(format_technicals_block(technicals))

    # 베타 분석 (벤치마크 fetch 실패 시 전체 skip — 0 fallback 시 아웃퍼폼 오판 방지)
    beta = get_beta()
    if price and price.prev_close > 0:
        benchmark_pct = _fetch_ticker_change(BETA_BENCHMARK)
        if benchmark_pct is not None:
            blocks.extend(format_beta_block({
                "actual_pct": price.change_pct,
                "qqq_pct": benchmark_pct,
            }))
        else:
            log.warning(f"{BETA_BENCHMARK} fetch 실패 — 베타 블록 스킵 (아웃퍼폼 오판 방지)")

    # Safety Margin: close 모드에서 항상 실행 (4% 조건 제거)
    sm = None
    if price and price.prev_close > 0 and closes:
        sm = check_safety_margin(
            closes,
            price.current,
            actual_pct=price.change_pct,
            beta=beta,
        )
        if sm:
            blocks.extend(format_safety_margin_block(sm))

    options = fetch_options_pcr()
    if options:
        blocks.extend(format_options_block(options))
        ws.setdefault("pcr_readings", []).append(options.pcr)

    short = fetch_short_interest()
    if short:
        blocks.extend(format_short_block(short))
        ws.setdefault("short_readings", []).append(short.short_pct)

    # App Store 순위 (전일 캐시 비교)
    prev_app_rank = None
    if APP_RANK_CACHE_FILE.exists():
        try:
            prev_app_rank = json.loads(APP_RANK_CACHE_FILE.read_text())
            yesterday = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
            if prev_app_rank.get("date") != yesterday:
                prev_app_rank = None
        except Exception:
            prev_app_rank = None
    curr_app_rank = fetch_appstore_rank()
    if curr_app_rank:
        blocks.extend(format_appstore_rank_block(prev_app_rank, curr_app_rank))

    company_filings = fetch_company_filings()
    new_filings = [filing for filing in company_filings
                   if filing.hash not in state.get("last_company_filing_hashes", [])]
    if new_filings:
        blocks.extend(format_company_filings_block(new_filings))
        remember_company_filings(state, new_filings)
        for filing in new_filings:
            ws.setdefault("company_filings", []).append(
                f"{filing.form}: {filing.description} ({filing.filing_date})"
            )
            ws.setdefault("thesis_impacts", []).append("risk")

    insider_trades = fetch_insider_trades()
    log.info(f"내부자 거래 fetch 결과: 총 {len(insider_trades)}건")
    new_insider_candidates = [t for t in insider_trades
                              if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                              not in state.get("last_insider_hashes", [])]
    new_insiders = material_insider_trades(new_insider_candidates)
    log.info(f"내부자 거래 신규: {len(new_insiders)}건 (중복 제외)")
    if new_insider_candidates:
        state["last_insider_hashes"] = [
            hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
            for t in insider_trades[:30]
        ]
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))
        for t in new_insiders:
            ws.setdefault("insider_trades", []).append(
                f"{t.trade_type}: {t.filer} {t.shares:,}주"
            )
    news = fetch_news()
    new_news = [item for item in news
                if item["hash"] not in state.get("last_news_hashes", [])]
    if new_news:
        new_news = translate_news(new_news)
        remember_analyzed_news(state, new_news)
    else:
        _set_source_health("AI 뉴스", "새 뉴스 없음")
    news_blocks = format_news_block(new_news)
    if news_blocks:
        blocks.extend(news_blocks)
        log.info(f"뉴스 블록 추가: {len(news_blocks)}개")
        for item in new_news:
            if not item.get("skip") and item.get("summary"):
                ws.setdefault("news_headlines", []).append(item["summary"])
                ws.setdefault("thesis_impacts", []).append(item.get("thesis_impact", "neutral"))
    else:
        log.info("표시할 관련 뉴스 없음")

    # DCA 기술지표 점수 (5-Layer, 100pts)
    dca_tech = calculate_dca_technical_score(ohlcv, weekly_ohlcv, sm=sm)
    if dca_tech:
        blocks.extend(format_dca_technical_block(dca_tech))

    blocks.insert(0, {"type": "header", "text": {"type": "plain_text",
        "text": f"🔔 {DISPLAY_TICKER} 장 마감 — {datetime.now(KST).strftime('%m/%d')}"}})
    summary_blocks = build_decision_summary_blocks(
        price=price,
        benchmark_pct=benchmark_pct,
        news=new_news,
        filings=new_filings,
        insiders=new_insiders,
        volume_activity=volume_activity,
    )
    for block in reversed(summary_blocks):
        blocks.insert(1, block)
    blocks.extend(_footer())
    send_slack(blocks)

    save_state(state)
    save_weekly_state(ws)
    log.info("Close done")


def run_morning():
    """호환성 명령. 반복 아침 알림은 제품 계약에 따라 발송하지 않는다."""
    log.info("=== MORNING ===")
    state = load_state()
    state["pending_morning_alert"] = None
    save_state(state)
    log.info("Morning alert disabled — no new decision value")


def run_13f():
    """13F 기관 포지션 (주 1회 토요일)"""
    log.info("=== 13F ===")
    state = load_state()
    filings = fetch_13f_filings()
    previous_positions = state.get("last_13f_positions", {})
    new_filings = [f for f in filings
                   if hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
                   not in state.get("last_13f_hashes", [])]

    for filing in new_filings:
        previous = previous_positions.get(filing.institution)
        if filing.shares <= 0:
            filing.change_type = "REPORTED"
        elif previous is None:
            filing.change_type = "BASELINE"
        elif filing.shares > previous:
            filing.change_type = "INCREASE"
        elif filing.shares < previous:
            filing.change_type = "DECREASE"
        else:
            filing.change_type = "UNCHANGED"

    if new_filings:
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"🏛 {DISPLAY_TICKER} 13F 기관 포지션 업데이트"}},
            {"type": "divider"},
        ]
        blocks.extend(format_13f_block(new_filings))
        blocks.extend(_footer())
        send_slack(blocks)

        state["last_13f_hashes"] = [
            hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
            for f in filings[:30]
        ]
        latest_positions = {}
        for filing in sorted(filings, key=lambda item: item.filing_date, reverse=True):
            if filing.shares > 0 and filing.institution not in latest_positions:
                latest_positions[filing.institution] = filing.shares
        state["last_13f_positions"] = latest_positions
        save_state(state)

    log.info(f"13F done — {len(new_filings)} new")


def run_weekly():
    """
    주간 브리핑 (매주 월 08:00 KST)
    BUG 4 FIX: 주가 숫자($) 완전 제거, DCA verdict 강조
    """
    log.info("=== WEEKLY ===")
    SOURCE_HEALTH.clear()
    ws = load_weekly_state()
    closes = fetch_price_history(60)
    ohlcv_w = fetch_ohlcv(days=210)
    volume_activity = calculate_volume_activity(ohlcv_w.get("volumes", []))
    _set_source_health("Yahoo 주간", "정상" if len(closes) >= 6 else "실패")
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    options = fetch_options_pcr()
    short = fetch_short_interest()

    # 실제 5거래일 비교. Slack에는 방향만 표시한다.
    weekly_pct = None
    if len(closes) >= 6 and closes[-6]:
        weekly_pct = (closes[-1] - closes[-6]) / closes[-6] * 100
    price = PriceData(change_pct=weekly_pct or 0.0) if weekly_pct is not None else None
    benchmark_closes = _fetch_yearly_closes(BETA_BENCHMARK)
    benchmark_pct = None
    if benchmark_closes and len(benchmark_closes) >= 6 and benchmark_closes[-6]:
        benchmark_pct = (benchmark_closes[-1] - benchmark_closes[-6]) / benchmark_closes[-6] * 100
    weekly_change_str = ""
    if weekly_pct is not None:
        weekly_change_str = (
            f"이번 주 방향: {_direction_label(weekly_pct)} | "
            f"{_relative_label(weekly_pct, benchmark_pct)}"
        )

    alerts = ws.get("alerts_fired", [])
    insider_summary = ws.get("insider_trades", [])
    news_summary = ws.get("news_headlines", [])
    company_filing_summary = ws.get("company_filings", [])
    thesis_impacts = ws.get("thesis_impacts", [])
    rsi_readings = ws.get("rsi_readings", [])
    pcr_readings = ws.get("pcr_readings", [])
    short_readings = ws.get("short_readings", [])

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"📋 {DISPLAY_TICKER} 주간 브리핑 — {datetime.now(KST).strftime('%m/%d')} 월"}},
        {"type": "divider"},
    ]

    # 주간 변동 요약: 정확한 가격과 수익률은 숨기고 방향만 표시한다.
    if weekly_change_str:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*📅 주간 방향*\n{weekly_change_str}"}})

    blocks.extend(format_technicals_block(technicals))
    blocks.extend(format_volume_activity_block(volume_activity, finalized=True))

    if pcr_readings:
        avg_pcr = sum(pcr_readings) / len(pcr_readings)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*📈 주간 PCR 평균: {avg_pcr:.3f}*" + (f" (현재 {options.pcr:.3f})" if options else "")}})

    if short_readings:
        avg_short = sum(short_readings) / len(short_readings)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*🩳 FINRA 일일 공매도 체결 비중 주간 평균: {avg_short:.1f}%*"
            + (f" (최신 {short.short_pct:.1f}%)" if short else "")}})

    if alerts:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*🚨 주간 알림*\n" + "\n".join(f"• {a}" for a in alerts[-8:])}})

    if insider_summary:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*🕴 주간 내부자 거래*\n" + "\n".join(f"• {t}" for t in insider_summary[-5:])}})

    if news_summary:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*📰 주간 주요 뉴스*\n" + "\n".join(f"• {h}" for h in news_summary[-5:])}})

    if company_filing_summary:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*🏛 주간 중요 SEC 공시*\n" + "\n".join(f"• {h}" for h in company_filing_summary[-5:])}})

    blocks.append({"type": "divider"})
    # 주간 기술지표 스코어
    weekly_ohlcv_w = fetch_weekly_ohlcv(weeks=40)
    if ohlcv_w:
        dca_tech_w = calculate_dca_technical_score(ohlcv_w, weekly_ohlcv_w, sm=None)
        if dca_tech_w:
            blocks.extend(format_dca_technical_block(dca_tech_w))
    synthetic_news = [
        {"summary": "주간 누적 사건", "thesis_impact": impact}
        for impact in thesis_impacts
    ]
    summary_blocks = build_decision_summary_blocks(
        price=price,
        benchmark_pct=benchmark_pct,
        news=synthetic_news,
        filings=company_filing_summary,
        volume_activity=volume_activity,
    )
    for block in reversed(summary_blocks):
        blocks.insert(1, block)
    blocks.extend(_footer())

    send_slack(blocks)

    save_weekly_state({
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "alerts_fired": [], "insider_trades": [], "news_headlines": [],
        "company_filings": [], "thesis_impacts": [],
        "rsi_readings": [], "pcr_readings": [], "short_readings": [],
    })
    log.info("Weekly done")


def run_dca_status():
    """
    DCA 현황 조회 — 현재 보유 수량 / 평단가 / 평가손익 Slack 전송
    workflow_dispatch: mode=dca_status
    """
    log.info("=== DCA STATUS ===")
    state = load_state()

    shares = state.get("dca_shares", 0.0)
    avg_price = state.get("dca_avg_price", 0.0)
    history = state.get("dca_history", [])

    if shares == 0 or avg_price == 0:
        send_slack([{"type": "section", "text": {"type": "mrkdwn", "text":
            "📭 아직 등록된 DCA 포지션이 없어요.\n"
            "Actions → Run workflow → mode: `dca_update` → 수량/가격 입력으로 추가하세요."}}])
        return

    lines = [
        f"*💼 {DISPLAY_TICKER} DCA 포지션 현황*",
        f"보유 수량: *{shares:,.1f}주*",
        f"평균 매수가: *${avg_price:.2f}*",
    ]

    if history:
        lines.append(f"\n*📋 매수 이력 (최근 5건)*")
        for h in history[-5:]:
            lines.append(f"• {h['date']} — {h['shares']:.1f}주 @ ${h['price']:.2f}")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"💼 {DISPLAY_TICKER} DCA 현황"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {"type": "divider"},
    ]
    send_slack(blocks)
    log.info(f"DCA status sent: {shares:.1f}주 @ ${avg_price:.2f}")


def run_dca_update():
    """
    DCA 추가매수 등록 — 새 매수 수량/가격 입력 시 평단가 재계산 후 Slack 전송
    workflow_dispatch inputs:
      DCA_SHARES: 매수 수량 (예: 10.5)
      DCA_PRICE:  매수가격 (예: 64.50)
    """
    log.info("=== DCA UPDATE ===")

    new_shares_str = os.environ.get("DCA_SHARES", "").strip()
    new_price_str = os.environ.get("DCA_PRICE", "").strip()

    if not new_shares_str or not new_price_str:
        log.error("DCA_SHARES 또는 DCA_PRICE 환경변수 없음")
        send_slack([{"type": "section", "text": {"type": "mrkdwn", "text":
            "❌ 입력값 오류 — DCA_SHARES와 DCA_PRICE를 모두 입력해주세요."}}])
        return

    try:
        new_shares = float(new_shares_str)
        new_price = float(new_price_str)
    except ValueError:
        send_slack([{"type": "section", "text": {"type": "mrkdwn", "text":
            f"❌ 숫자 변환 실패 — shares: `{new_shares_str}`, price: `{new_price_str}`\n"
            "숫자만 입력해주세요 (예: 10.5 / 64.50)"}}])
        return

    state = load_state()
    prev_shares = state.get("dca_shares", 0.0)
    prev_avg = state.get("dca_avg_price", 0.0)
    history = state.get("dca_history", [])

    # 가중평균 재계산
    if prev_shares == 0 or prev_avg == 0:
        new_avg = new_price
        total_shares = new_shares
        is_first = True
    else:
        total_shares = prev_shares + new_shares
        new_avg = (prev_shares * prev_avg + new_shares * new_price) / total_shares
        is_first = False

    # 물타기 / 불타기 판단
    if not is_first:
        if new_price < prev_avg:
            action = "🧊 물타기"
        elif new_price > prev_avg:
            action = "🔥 불타기"
        else:
            action = "➡️ 동일가 매수"
    else:
        action = "🆕 최초 등록"

    # 이력 추가
    history.append({
        "date": datetime.now(KST).strftime("%Y-%m-%d"),
        "shares": new_shares,
        "price": new_price,
        "action": action,
    })

    state["dca_shares"] = round(total_shares, 4)
    state["dca_avg_price"] = round(new_avg, 4)
    state["dca_history"] = history
    save_state(state)

    lines = [
        f"*{action}* 등록 완료!",
        f"",
        f"이번 매수: {new_shares:.1f}주 @ ${new_price:.2f}",
        f"",
        f"*업데이트된 포지션*",
        f"총 보유: *{total_shares:.1f}주*",
        f"새 평단가: *${new_avg:.2f}*",
    ]

    if not is_first:
        avg_change = new_avg - prev_avg
        lines.append(f"평단 변화: ${prev_avg:.2f} → ${new_avg:.2f} ({avg_change:+.2f})")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "✅ DCA 포지션 업데이트"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {"type": "divider"},
    ]
    send_slack(blocks)
    log.info(f"DCA updated: {total_shares:.1f}주 @ ${new_avg:.2f} ({action})")


# ─────────────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────────────
def main():
    mode = os.environ.get("RUN_MODE", sys.argv[1] if len(sys.argv) > 1 else "normal").lower()
    log.info(f"{TICKER} Monitor v3.2 — mode: {mode} | {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}")
    {
        "normal": run_normal,
        "close": run_close,
        "morning": run_morning,
        "13f": run_13f,
        "weekly": run_weekly,
        "dca_status": run_dca_status,
        "dca_update": run_dca_update,
    }.get(mode, lambda: log.error(f"Unknown mode: {mode}"))()


if __name__ == "__main__":
    main()
