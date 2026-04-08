#!/usr/bin/env python3
"""
$HOOD Advanced Monitor v3.2
============================
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
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

import requests

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
TICKER = "HOOD"
CIK = "0001783879"          # Robinhood Markets, Inc.
CIK_PADDED = CIK            # 10자리 (선행 0 포함)
CIK_SHORT = CIK.lstrip("0") # 선행 0 제거

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STATE_FILE = Path("state.json")
WEEKLY_STATE_FILE = Path("weekly_state.json")

YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
YAHOO_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

SEC_HEADERS = {"User-Agent": "HoodMonitor/3.1 contact@example.com"}
FINRA_SHORT_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hood_monitor")


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
    market_state: str = ""    # "REGULAR" | "PRE" | "POST" | "CLOSED"
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
class DCASignal:
    score: int = 50
    summary: str = ""
    verdict: str = ""       # 명확한 결론 한 줄
    factors: list = field(default_factory=list)


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
        "price_history": [],
        "price_alert_max_pct": 0,
        "price_alert_direction": "",
        "price_alert_date": "",
        "pending_morning_alert": None,
        # ── DCA 포트폴리오 ──
        "dca_shares": 0.0,        # 총 보유 수량
        "dca_avg_price": 0.0,     # 평균 매수가
        "dca_history": [],        # 매수 이력
    }


def save_state(state: dict):
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
        "rsi_readings": [],
        "pcr_readings": [],
        "short_readings": [],
    }


def save_weekly_state(ws: dict):
    WEEKLY_STATE_FILE.write_text(json.dumps(ws, indent=2, ensure_ascii=False))


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


# ─────────────────────────────────────────────
# 1. 주가 데이터
# ─────────────────────────────────────────────
def fetch_price(realtime: bool = True) -> Optional[PriceData]:
    """
    시장 상태별 정확한 현재가/전일종가 계산.

    realtime=True  (run_normal): 프리/정규/애프터 실시간 가격
    realtime=False (run_close):  확정 일봉만 사용 (오늘 정규장 종가 vs 어제 정규장 종가)
    """
    _yahoo_throttle()
    url = YAHOO_QUOTE_URL.format(ticker=TICKER)

    # ── 1. 확정 일봉 (prev_close 기준용) ─────────────────────────
    resp_1d = safe_get(url, params={"interval": "1d", "range": "10d"})
    if not resp_1d:
        return None
    try:
        closes_daily = [
            c for c in resp_1d.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            if c is not None
        ]
        volumes_daily = [
            v for v in resp_1d.json()["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
            if v is not None
        ]
    except Exception as e:
        log.error(f"fetch_price 일봉 파싱 실패: {e}")
        return None

    if len(closes_daily) < 2:
        log.warning("fetch_price: 일봉 데이터 부족")
        return None

    # ── 2. 장 상태 판별 (UTC 시간 기준) ──────────────────────────
    now_utc = datetime.now(UTC)
    hm      = now_utc.hour * 60 + now_utc.minute
    dow     = now_utc.weekday()  # 0=Mon 6=Sun

    if dow >= 5:                                    # 주말
        market_state = "CLOSED"
    elif hm < 8 * 60:                              # UTC 00:00~08:00
        market_state = "CLOSED"
    elif hm < 13 * 60 + 30:                        # UTC 08:00~13:30 → PRE
        market_state = "PRE"
    elif hm < 20 * 60:                             # UTC 13:30~20:00 → REGULAR
        market_state = "REGULAR"
    elif hm < 24 * 60:                             # UTC 20:00~24:00 → POST
        market_state = "POST"
    else:
        market_state = "CLOSED"

    # ── 3. 현재가 & 전일종가 ─────────────────────────────────────
    if realtime and market_state in ("PRE", "REGULAR", "POST"):
        # 실시간: 1분봉 최신 바
        resp_1m = safe_get(url, params={"interval": "1m", "range": "2d", "includePrePost": "true"})
        if not resp_1m:
            return None
        try:
            result_1m  = resp_1m.json()["chart"]["result"][0]
            timestamps = result_1m.get("timestamp", [])
            closes_1m  = result_1m["indicators"]["quote"][0].get("close", [])
            today      = now_utc.date()
            current    = None
            for i in range(len(timestamps) - 1, -1, -1):
                if i < len(closes_1m) and closes_1m[i] is not None:
                    if datetime.fromtimestamp(timestamps[i], UTC).date() == today:
                        current = round(float(closes_1m[i]), 2)
                        break
        except Exception as e:
            log.error(f"fetch_price 1분봉 파싱 실패: {e}")
            return None

        if current is None:
            log.warning("fetch_price: 오늘 1분봉 바 없음")
            return None

        prev_close = round(float(closes_daily[-1]), 2)   # 전일 확정 종가
        today_vol  = int(sum(
            v for i, v in enumerate(result_1m["indicators"]["quote"][0].get("volume", []))
            if v and i < len(timestamps)
            and datetime.fromtimestamp(timestamps[i], UTC).date() == today
        ))
    else:
        # CLOSED: 확정 일봉 사용
        current    = round(float(closes_daily[-1]), 2)
        prev_close = round(float(closes_daily[-2]), 2)
        today_vol  = int(volumes_daily[-1]) if volumes_daily else 0

    change_pct = round((current - prev_close) / prev_close * 100, 2) if prev_close else 0

    # 5일 평균 거래량
    past_vols  = volumes_daily[:-1] if len(volumes_daily) > 1 else []
    vol_avg_5d = int(sum(past_vols[-5:]) / len(past_vols[-5:])) if past_vols else 0

    log.info(
        f"fetch_price: state={market_state} "
        f"current={current:.2f} prev={prev_close:.2f} chg={change_pct:+.2f}%"
    )

    try:
        meta = resp_1d.json()["chart"]["result"][0]["meta"]
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
        market_state=market_state,
        timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
    )


def fetch_price_history(days: int = 60) -> list:
    _yahoo_throttle()
    url = YAHOO_QUOTE_URL.format(ticker=TICKER)
    resp = safe_get(url, params={"interval": "1d", "range": f"{days}d"})
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
BETA_CACHE_FILE = Path("beta_cache.json")
BETA_BENCHMARK = "QQQ"


def _fetch_yearly_closes(ticker: str) -> list:
    """1년치 일간 종가 반환"""
    _yahoo_throttle()
    url = YAHOO_QUOTE_URL.format(ticker=ticker)
    resp = safe_get(url, params={"interval": "1d", "range": "1y"})
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
    rs = [(stock_closes[i] - stock_closes[i-1]) / stock_closes[i-1]
          for i in range(n - min(n, len(stock_closes)), n)
          if stock_closes[i-1] != 0]
    rm = [(market_closes[i] - market_closes[i-1]) / market_closes[i-1]
          for i in range(n - min(n, len(market_closes)), n)
          if market_closes[i-1] != 0]

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
    BETA_CACHE_FILE.write_text(json.dumps({"date": today, "beta": beta}, indent=2))
    log.info(f"Beta 계산 완료: β={beta} → 캐시 저장")
    return beta


def calc_beta_divergence(beta: float, market_pct: float, actual_pct: float) -> dict:
    """
    기대 수익률 vs 실제 수익률 이격도 + 피어 그룹 비교 통합.
    피어($COIN, $MSTR) 데이터는 여기서 fetch하여 dict에 포함.
    """
    expected = round(beta * market_pct, 2)
    divergence = round(actual_pct - expected, 2)

    # 피어 데이터 fetch (상대 강도 블록에 통합 표시)
    coin_pct = _fetch_ticker_change("COIN") or 0.0
    mstr_pct = _fetch_ticker_change("MSTR") or 0.0

    # 피어 평균 대비 HOOD 이격
    peer_avg = round((coin_pct + mstr_pct) / 2, 2)
    peer_diff = round(actual_pct - peer_avg, 2)

    return {
        "beta": beta,
        "qqq_pct": market_pct,
        "expected_pct": expected,
        "actual_pct": actual_pct,
        "divergence": divergence,
        "coin_pct": coin_pct,
        "mstr_pct": mstr_pct,
        "peer_avg": peer_avg,
        "peer_diff": peer_diff,
    }


def format_beta_block(bd: dict) -> list:
    """상대 강도 통합 블록 — 지수 대비(β 기반) + 피어 대비($COIN/$MSTR)"""
    div      = bd["divergence"]
    beta     = bd["beta"]
    expected = bd["expected_pct"]
    actual   = bd["actual_pct"]
    qqq_pct  = bd["qqq_pct"]
    peer_diff = bd["peer_diff"]
    peer_avg  = bd["peer_avg"]
    coin_pct  = bd["coin_pct"]
    mstr_pct  = bd["mstr_pct"]

    # ── 지수 대비 (β 기반) ──
    if div >= 3:
        vs_index = "🟢 *아웃퍼폼*"
        vs_index_desc = f"β 기대치보다 {div:+.1f}%p 초과 상승"
    elif div >= 1:
        vs_index = "🟡 *소폭 아웃퍼폼*"
        vs_index_desc = f"기대 범위 상단 ({div:+.1f}%p)"
    elif div <= -3:
        vs_index = "🔴 *언더퍼폼*"
        vs_index_desc = f"β 기대치보다 {div:+.1f}%p 초과 하락"
    elif div <= -1:
        vs_index = "🟠 *소폭 언더퍼폼*"
        vs_index_desc = f"기대 범위 하단 ({div:+.1f}%p)"
    else:
        vs_index = "⚪ *기대 범위 내*"
        vs_index_desc = f"시장 움직임과 부합 ({div:+.1f}%p)"

    # ── 피어 대비 ($COIN / $MSTR) ──
    if peer_diff >= 2:
        vs_peer = "🟢 *피어 아웃퍼폼*"
        vs_peer_desc = f"COIN/MSTR 평균보다 {peer_diff:+.1f}%p 강세"
    elif peer_diff >= 0.5:
        vs_peer = "🟡 *피어 소폭 강세*"
        vs_peer_desc = f"피어 평균 대비 {peer_diff:+.1f}%p"
    elif peer_diff <= -2:
        vs_peer = "🔴 *피어 언더퍼폼*"
        vs_peer_desc = f"COIN/MSTR 평균보다 {peer_diff:+.1f}%p 약세"
    elif peer_diff <= -0.5:
        vs_peer = "🟠 *피어 소폭 약세*"
        vs_peer_desc = f"피어 평균 대비 {peer_diff:+.1f}%p"
    else:
        vs_peer = "⚪ *피어 동조*"
        vs_peer_desc = f"피어 평균과 유사 ({peer_diff:+.1f}%p)"

    return [
        _sec(
            f"*📐 상대 강도*  β = *{beta:.2f}*\n"
            f"지수 대비: {vs_index} — {vs_index_desc}\n"
            f"피어 대비: {vs_peer} — {vs_peer_desc}"
        ),
        _ctx(
            f"*기대수익률 (HOOD) {expected:+.2f}%* (β×QQQ)  |  "
            f"*실제수익률 (HOOD) {actual:+.2f}%*  |  "
            f"*QQQ {qqq_pct:+.2f}%*"
        ),
        _ctx(
            f"피어: *COIN {coin_pct:+.2f}%*  |  *MSTR {mstr_pct:+.2f}%*  |  평균 {peer_avg:+.2f}%"
        ),
    ]


# ─────────────────────────────────────────────
# 1-2. 상대 강도 유틸 (_fetch_ticker_change 공용)
# ─────────────────────────────────────────────


def _fetch_ticker_change(ticker: str) -> Optional[float]:
    """
    전일 정규장 종가 대비 현재가 변동률.
    fetch_price와 동일한 실증된 방식:
    - includePrePost=true로 오늘 프리/정규/애프터 quotes 포함
    - current = 오늘 날짜 최신 바
    - prev    = meta.regularMarketPrice (전일 정규장 종가)
    """
    _yahoo_throttle()
    url = YAHOO_QUOTE_URL.format(ticker=ticker)
    resp = safe_get(url, params={"interval": "1m", "range": "2d", "includePrePost": "true"})
    if not resp:
        return None
    try:
        data      = resp.json()
        result    = data["chart"]["result"][0]
        meta      = result["meta"]
        timestamps = result.get("timestamp", [])
        closes    = result["indicators"]["quote"][0].get("close", [])

        today   = datetime.now(UTC).date()
        current = None
        for i in range(len(timestamps) - 1, -1, -1):
            if i < len(closes) and closes[i] is not None:
                if datetime.fromtimestamp(timestamps[i], UTC).date() == today:
                    current = float(closes[i])
                    break

        prev = float(meta.get("regularMarketPrice") or 0)

        if not current or not prev:
            return None
        return round((current - prev) / prev * 100, 2)
    except Exception as e:
        log.debug(f"_fetch_ticker_change({ticker}) error: {e}")
        return None


# ─────────────────────────────────────────────
# 1-5. HOOD × BTC 30일 상관계수
# ─────────────────────────────────────────────
def calc_btc_correlation() -> Optional[dict]:
    """
    HOOD와 BTC-USD의 최근 30일 일간 수익률 피어슨 상관계수 계산.
    r > 0.7 → 강한 양의 상관 (BTC 따라감)
    r < 0.3 → 독립적 움직임
    """
    log.info("BTC 상관계수 계산 시작...")

    def fetch_30d_returns(ticker: str) -> Optional[list]:
        _yahoo_throttle()
        url = YAHOO_QUOTE_URL.format(ticker=ticker)
        resp = safe_get(url, params={"interval": "1d", "range": "35d"})
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
        sig_line = f"🔗 *강한 BTC 연동* (r={corr:.2f}) — BTC 방향이 HOOD를 견인"
    elif bd["signal"] == "moderate":
        sig_line = f"🔗 *중간 연동* (r={corr:.2f}) — BTC와 부분적으로 동조"
    else:
        sig_line = f"🔓 *낮은 연동* (r={corr:.2f}) — HOOD 개별 팩터 우세"

    # BTC 방향 해석
    if bd["signal"] == "high" and btc >= 2:
        interp = "BTC 상승 → HOOD 상승 압력 가능"
    elif bd["signal"] == "high" and btc <= -2:
        interp = "BTC 하락 → HOOD 하락 압력 가능"
    else:
        interp = ""

    ctx = f"30일 기준 | *BTC 당일 {btc:+.2f}%*"
    if interp:
        ctx += f"  |  {interp}"

    return [
        _sec(f"*₿ HOOD × BTC 상관계수*\n{sig_line}"),
        _ctx(ctx),
    ]


# ─────────────────────────────────────────────
# 1-6. Apple App Store 순위 트래킹
# ─────────────────────────────────────────────
ROBINHOOD_APP_ID = "938003185"    # App Store ID (미국 기준)
APP_RANK_CACHE_FILE = Path("app_rank_cache.json")


def fetch_appstore_rank() -> Optional[dict]:
    """
    Apple App Store 미국 금융 카테고리 상위 200개 무료 앱 RSS에서
    로빈후드 순위를 파싱.
    공식 Apple RSS: rss.applemarketingtools.com
    """
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
                if app.get("id") == ROBINHOOD_APP_ID:
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
                if app.get("id") == ROBINHOOD_APP_ID:
                    result["rank_overall"] = i + 1
                    break
        except Exception as e:
            log.debug(f"App Store overall parse: {e}")

    log.info(f"App Store 순위: Finance #{result['rank_finance']} | Overall #{result['rank_overall']}")
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
    url = YAHOO_QUOTE_URL.format(ticker=ticker)
    resp = safe_get(url, params={"interval": "1m", "range": range_str})
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
    divergence_warning: bool = False   # 피어 반등 중 HOOD만 하락 가속
    peer_coin_pct: float = 0.0
    peer_mstr_pct: float = 0.0
    dca_attraction: int = 0            # DCA 매력도 1~10


def check_safety_margin(
    closes_daily: list,
    current_price: float,
    actual_pct: float = 0.0,   # HOOD 당일 등락률
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

    # ── 4. 피어 그룹 분기 감지 ($COIN, $MSTR) ──────
    peer_coin_pct = _fetch_ticker_change("COIN") or 0.0
    peer_mstr_pct = _fetch_ticker_change("MSTR") or 0.0

    # HOOD가 하락 가속 중인데 피어 둘 다 양전 → Divergence Warning
    divergence_warning = (
        momentum_signal == "accelerating"
        and actual_pct < -2
        and peer_coin_pct > 0
        and peer_mstr_pct > 0
    )
    if divergence_warning:
        log.info(f"Divergence Warning: HOOD {actual_pct:+.2f}% 하락 가속 | COIN {peer_coin_pct:+.2f}% / MSTR {peer_mstr_pct:+.2f}% 반등")

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
        return []
    try:
        root = ET.fromstring(resp.text)
        news = []
        for item in root.findall(".//item")[:15]:
            title    = item.findtext("title", "")
            pub_date = item.findtext("pubDate", "")
            link     = item.findtext("link", "")
            news.append({
                "title": title,
                "date":  pub_date,
                "link":  link,
                "hash":  hashlib.md5(title.encode()).hexdigest()[:12],
            })
        return news
    except Exception as e:
        log.error(f"News parse error: {e}")
        return []


def _fetch_article_body(url: str, max_chars: int = 600) -> str:
    """기사 URL에서 본문 앞부분 발췌 (파싱 실패 시 빈 문자열)"""
    if not url:
        return ""
    try:
        resp = safe_get(url, timeout=8, retries=1)
        if not resp:
            return ""
        import re
        # script/style 제거 후 태그 제거, 공백 정리
        text = re.sub(r"<script[^>]*>.*?</script>", "", resp.text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>",  "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        # 의미 있는 본문 시작점 찾기 (짧은 메타 텍스트 스킵)
        sentences = [s.strip() for s in text.split(".") if len(s.strip()) > 40]
        body = ". ".join(sentences[:6])
        return body[:max_chars]
    except Exception:
        return ""


def translate_news(news: list) -> list:
    """
    HOOD 관련 뉴스 필터링 + 한국어 요약(15자) + 기사 핵심 번역(2~3문장)
    """
    if not news or not ANTHROPIC_API_KEY:
        log.info(f"translate_news skip — news:{len(news)} api_key:{'있음' if ANTHROPIC_API_KEY else '없음'}")
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
    prompt = f"""당신은 $HOOD(Robinhood Markets) 투자 알림 봇입니다.
아래 뉴스 목록(제목+본문 발췌)을 분석해주세요.

규칙:
1. Robinhood Markets / $HOOD 주가에 직접 영향을 주는 뉴스만 포함
2. 포함 기준: 실적, 규제, 경쟁사 직접 비교, 경영진 변동, 주요 제품/서비스, 기관 매수/매도, 소송
3. 제외 기준: 증권업 일반 뉴스, 금리 일반론, 다른 회사 뉴스에 HOOD가 언급만 된 경우
4. 반드시 한국어로만 출력

각 뉴스에 대해 JSON 배열로만 응답 (다른 텍스트 없이):
[
  {{
    "idx": 1,
    "relevant": true,
    "summary": "15자 이내 핵심 요약",
    "translation": "기사 핵심 내용을 2~3문장으로 자연스러운 한국어로 번역. 투자자 관점에서 중요한 수치·사실 위주로.",
    "sentiment": "positive|negative|neutral"
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
                "max_tokens": 1500,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=40,
        )
        if resp.status_code != 200:
            log.error(f"Claude API 오류 (뉴스 번역): HTTP {resp.status_code} — {resp.text[:200]}")
            return news

        text = ""
        for block in resp.json().get("content", []):
            if block.get("type") == "text":
                text += block["text"]
        text = text.strip().replace("```json", "").replace("```", "").strip()
        results = json.loads(text)

        relevant = sum(1 for r in results if r.get("relevant", False))
        skipped  = len(results) - relevant
        log.info(f"뉴스 번역 완료: 총 {len(results)}건 — 관련 {relevant}건 / 스킵 {skipped}건")

        for item in results:
            idx = item.get("idx", 0) - 1
            if not (0 <= idx < len(news)):
                continue
            if not item.get("relevant", False):
                news[idx]["skip"] = True
            else:
                news[idx]["summary"]     = item.get("summary", "")
                news[idx]["translation"] = item.get("translation", "")
                news[idx]["sentiment"]   = item.get("sentiment", "neutral")
    except Exception as e:
        log.warning(f"뉴스 번역 예외: {e}")

    return news


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
# 6. 내부자 거래 (Form 4) — EDGAR atom feed 방식
# ─────────────────────────────────────────────
def fetch_insider_trades() -> list:
    """
    EDGAR company search atom feed로 Form 4 목록을 가져온 뒤
    각 filing의 index 페이지에서 원본 XML을 직접 찾아 파싱.
    filing agent CIK 경로 404 문제 해결.
    """
    trades = []
    try:
        # atom feed: Robinhood(CIK)가 issuer인 Form 4 목록
        resp = safe_get(
            "https://www.sec.gov/cgi-bin/browse-edgar",
            headers=SEC_HEADERS,
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
            idx_resp = safe_get(filing_href, headers=SEC_HEADERS, retries=1)
            if not idx_resp:
                log.warning(f"Form 4 index 응답 없음: {filing_href}")
                continue

            xml_url = _find_form4_xml_url(idx_resp.text, filing_href)
            if not xml_url:
                log.warning(f"Form 4 XML 링크 미발견 (index HTML 길이={len(idx_resp.text)}): {filing_href}")
                continue

            log.info(f"Form 4 XML 요청: {xml_url}")
            xml_resp = safe_get(xml_url, headers=SEC_HEADERS, retries=1)
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
        log.error(f"Insider fetch error: {e}")
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
        # D = 처분 (Disposition, 비시장 매도 등) → 약세 시그널
        if txn_code == "P":
            trade_type = "Purchase"
        elif txn_code == "S":
            trade_type = "Sale"
        elif txn_code == "A":
            trade_type = "Award"     # RSU 귀속 / 스톡옵션 부여
        elif txn_code == "D" or acq == "D":
            trade_type = "Sale"
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
    - 실제 13F XML에서 HOOD 보유 주식 수 / 평가금액 파싱
    """
    filings = []
    try:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
        start_date = (datetime.now(UTC) - timedelta(days=120)).strftime("%Y-%m-%d")

        # 수정된 EDGAR full-text search URL
        search_url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": '"Robinhood Markets"',
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

            # infoTable XML 파싱으로 HOOD 포지션 추출
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
    13F infoTable XML에서 HOOD 포지션(주식 수, 평가금액) 추출
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
    """infoTable XML에서 HOOD 항목 찾아 주식 수와 평가금액 추출"""
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
            # HOOD 또는 Robinhood 포함 여부 확인
            if "HOOD" not in name.upper() and "ROBINHOOD" not in name.upper():
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
def calculate_dca_signal(price, technicals, options, short_interest, insider_trades, news, sm=None) -> DCASignal:
    score, factors = _rule_based_dca_score(price, technicals, options, short_interest, insider_trades, sm=sm)
    log.info(f"DCA 규칙기반 점수: {score}/100")

    if ANTHROPIC_API_KEY:
        log.info("Claude API 호출: DCA 분석")
        try:
            ai = _claude_dca_analysis(technicals, options, short_interest, insider_trades, news, score, factors)
            if ai:
                log.info(f"DCA AI 분석 완료: {ai.score}/100 — {ai.verdict}")
                return ai
        except Exception as e:
            log.warning(f"Claude DCA fallback: {e}")
    else:
        log.info("ANTHROPIC_API_KEY 없음 — 규칙기반 DCA 사용")

    return DCASignal(
        score=score,
        verdict=_score_to_verdict(score),
        summary=_score_to_summary(score),
        factors=factors,
    )


def _rule_based_dca_score(price, technicals, options, short_interest, insider_trades, sm=None):
    score = 50
    factors = []

    # ── RSI ──────────────────────────────────────
    if technicals.rsi_14 <= 30:
        bonus = min(20, int((30 - technicals.rsi_14) * 1.5))
        score += bonus
        factors.append(f"🟢 RSI {technicals.rsi_14} 과매도 구간 (+{bonus})")
    elif technicals.rsi_14 <= 40:
        score += 8
        factors.append(f"🟡 RSI {technicals.rsi_14} 매수 관심 구간 (+8)")
    elif technicals.rsi_14 >= 70:
        penalty = min(15, int((technicals.rsi_14 - 70) * 1.0))
        score -= penalty
        factors.append(f"🔴 RSI {technicals.rsi_14} 과매수 구간 (-{penalty})")

    # ── MACD ─────────────────────────────────────
    if technicals.macd_alert == "bullish_cross":
        score += 10
        factors.append("🟢 MACD 골든크로스 (+10)")
    elif technicals.macd_alert == "bearish_cross":
        score -= 10
        factors.append("🔴 MACD 데드크로스 (-10)")

    # ── 옵션 PCR ─────────────────────────────────
    if options:
        if options.pcr > 1.2:
            score += 8
            factors.append(f"🟢 PCR {options.pcr:.2f} 풋 헤징 집중 → 역발상 매수 (+8)")
        elif options.pcr < 0.5:
            score -= 5
            factors.append(f"🔴 PCR {options.pcr:.2f} 과도한 낙관 (-5)")

    # ── 공매도 ───────────────────────────────────
    if short_interest:
        if short_interest.short_pct > 60:
            score += 8
            factors.append(f"🟢 공매도 {short_interest.short_pct:.1f}% 숏스퀴즈 가능 (+8)")
        elif short_interest.short_pct > 50:
            score += 3
            factors.append(f"🟡 공매도 {short_interest.short_pct:.1f}% 높은 편 (+3)")

    # ── 내부자 ───────────────────────────────────
    recent_buys = [t for t in insider_trades if t.trade_type == "Purchase" and _is_recent(t.date, 30)]
    recent_sells = [t for t in insider_trades if t.trade_type == "Sale" and _is_recent(t.date, 30)]
    buy_val = sum(t.total_value for t in recent_buys)
    sell_val = sum(t.total_value for t in recent_sells)
    if recent_buys and buy_val > 100_000:
        score += 10
        factors.append(f"🟢 내부자 매수 {len(recent_buys)}건 (대규모) (+10)")
    elif len(recent_sells) > 2 and sell_val > 1_000_000:
        score -= 8
        factors.append(f"🔴 내부자 대량 매도 {len(recent_sells)}건 (-8)")

    # ── 가격 변동 ────────────────────────────────
    if price and price.change_pct <= -5:
        score += 5
        factors.append(f"🟢 큰 폭 하락 → 매수 기회 (+5)")
    elif price and price.change_pct >= 5:
        score -= 3
        factors.append(f"🟡 큰 폭 상승 → 추격 주의 (-3)")

    # ── Safety Margin 3요소 (SM 있을 때만) ────────
    if sm is not None:
        # 1) 볼린저 밴드 이탈
        if sm.bb_signal == "extreme_oversold":
            score += 12
            factors.append(f"🟢 BB 하단 이탈 (Extreme Oversold, 하단 대비 {sm.pct_from_lower:+.1f}%) (+12)")
        elif sm.bb_signal == "oversold":
            score += 6
            factors.append(f"🟡 BB 하단 근접 ({sm.pct_from_lower:+.1f}%) (+6)")
        elif sm.bb_signal == "overbought":
            score -= 8
            factors.append(f"🔴 BB 상단 돌파 — 과열 (-8)")

        # 2) 베타 대비 초과 이탈
        if sm.beta_excess_pct <= -5:
            score += 12
            factors.append(f"🟢 β 대비 과도 하락 ({sm.beta_excess_pct:+.1f}%p) — 통계적 반등 가능 (+12)")
        elif sm.beta_excess_pct <= -2:
            score += 6
            factors.append(f"🟡 β 대비 소폭 초과 하락 ({sm.beta_excess_pct:+.1f}%p) (+6)")
        elif sm.beta_excess_pct >= 5:
            score -= 5
            factors.append(f"🔴 β 대비 과도 상승 ({sm.beta_excess_pct:+.1f}%p) (-5)")

        # 3) 피어 분기 / 모멘텀 (리스크 요소)
        if sm.divergence_warning:
            score -= 8
            factors.append(f"🔴 Divergence Warning — 피어 반등 중 HOOD 하락 가속 (-8)")
        elif sm.momentum_signal == "accelerating":
            score -= 4
            factors.append(f"🟠 하락 가속 중 — 추가 하락 리스크 (-4)")
        elif sm.momentum_signal == "decelerating":
            score += 4
            factors.append(f"🟢 하락 둔화 — 저점 탐색 신호 (+4)")

    return max(0, min(100, score)), factors


def _claude_dca_analysis(technicals, options, short_interest, insider_trades, news, rule_score, rule_factors):
    """BUG 4 FIX: 가격 숫자 제거, 결론(verdict) 명확화"""
    context = f"""당신은 $HOOD(Robinhood Markets) DCA 장기 투자자를 위한 시그널 분석가입니다.
주가 숫자는 알려주지 않습니다 (투자자 요청). 기술적/시장 지표만으로 분석해주세요.

현재 지표:
- RSI(14): {technicals.rsi_14} ({'과매도' if technicals.rsi_14 <= 30 else '과매수' if technicals.rsi_14 >= 70 else '중립'})
- MACD 히스토그램: {technicals.macd_histogram:.4f} (양수=상승모멘텀)
- MACD 시그널: {technicals.macd_alert or "없음"}
- 옵션 PCR: {f"{options.pcr:.3f} ({options.pcr_signal})" if options else "N/A"}
- 공매도 비율: {f"{short_interest.short_pct:.1f}%" if short_interest else "N/A"}
- 내부자 매수: {sum(1 for t in insider_trades if t.trade_type == "Purchase")}건 / 매도: {sum(1 for t in insider_trades if t.trade_type == "Sale")}건
- 규칙기반 DCA 점수: {rule_score}/100

규칙기반 분석:
{chr(10).join(rule_factors)}

최근 뉴스 (한국어):
{chr(10).join(f"- {n.get('summary', n['title'])}" for n in news if not n.get('skip') and n.get('summary'))[:5]}

다음 JSON으로만 응답:
{{"score": 0-100, "verdict": "지금 DCA 추가매수를 고려할 만한가에 대한 한 줄 결론 (예: '추가매수 우호적 — 과매도 구간, 내부자 매수 감지')", "summary": "2문장 한국어 설명", "factors": ["핵심요인1", "핵심요인2", "핵심요인3"]}}"""

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": context}],
        },
        timeout=30,
    )
    if resp.status_code != 200:
        log.error(f"Claude API 오류 (DCA 분석): HTTP {resp.status_code} — {resp.text[:200]}")
        return None

    text = ""
    for block in resp.json().get("content", []):
        if block.get("type") == "text":
            text += block["text"]
    text = text.strip().replace("```json", "").replace("```", "").strip()
    result = json.loads(text)

    return DCASignal(
        score=max(0, min(100, int(result.get("score", rule_score)))),
        verdict=result.get("verdict", _score_to_verdict(rule_score)),
        summary=result.get("summary", _score_to_summary(rule_score)),
        factors=result.get("factors", rule_factors),
    )


def _score_to_verdict(score: int) -> str:
    """BUG 4 FIX: DCA 투자자에게 명확한 결론 한 줄"""
    if score >= 75:
        return "✅ 추가매수 적극 고려 — 다수 지표가 매수 우호적"
    elif score >= 60:
        return "🟡 추가매수 고려 가능 — 일부 긍정 시그널"
    elif score >= 45:
        return "⚪ 정기 DCA 유지 — 특별한 추가매수 이유 없음"
    elif score >= 30:
        return "🟠 관망 권장 — 부정적 시그널 우세"
    else:
        return "🔴 추가매수 자제 — 다수 부정 시그널"


def _score_to_summary(score: int) -> str:
    if score >= 75:
        return "기술적/시장 지표 다수가 매수 우호적 환경을 가리킵니다. DCA 추가매수를 적극 검토할 시점입니다."
    elif score >= 60:
        return "일부 긍정적 시그널이 감지됩니다. 정기 DCA에 소량 추가를 고려할 수 있습니다."
    elif score >= 45:
        return "특별한 방향성 없음. 정기 DCA 일정을 그대로 유지하세요."
    elif score >= 30:
        return "부정적 시그널이 다소 우세합니다. 추가매수보다 관망을 권장합니다."
    else:
        return "여러 지표가 부정적. 추가매수를 서두르지 마세요."


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


def format_technicals_block(ts: TechnicalSignals) -> list:
    if ts.rsi_14 <= 30:
        rsi_line = f"🟢 *RSI {ts.rsi_14}* — 과매도, DCA 타이밍 가능"
    elif ts.rsi_14 <= 40:
        rsi_line = f"🟡 *RSI {ts.rsi_14}* — 약세, 매수 관심"
    elif ts.rsi_14 >= 70:
        rsi_line = f"🔴 *RSI {ts.rsi_14}* — 과열, 추격 자제"
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
    emoji = "🔴" if si.signal == "high_short" else "⚪"
    return [
        _sec(f"*🩳 공매도*  {emoji} *{si.short_pct:.1f}%*"),
        _ctx(f"기준일 {si.date} | 공매도 {si.short_volume:,} / 총 {si.total_volume:,}"),
    ]


def format_insider_block(trades: list) -> list:
    if not trades:
        return []

    # 거래 유형별 표시 맵
    TYPE_MAP = {
        "Purchase": ("🟢", "장내 매수"),
        "Sale":     ("🔴", "장내 매도"),
        "Award":    ("🔵", "RSU 귀속"),
    }
    # txn_code 보조 레이블 (Purchase/Sale 내에서 세분화)
    CODE_LABEL = {
        "P": "장내 매수",
        "S": "장내 매도",
        "A": "RSU 귀속",
        "D": "처분 매도",
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
        price_str = f" @ ${t.price:.2f}" if t.price > 0 and t.trade_type != "Award" else ""

        lines.append(
            f"{emoji} *{t.filer}*{title_str}\n"
            f"   {type_label}  {t.shares:,}주{price_str}  {scale}  _{t.date}_"
        )

    return [
        _sec("*🕴 내부자 거래*\n" + "\n".join(lines)),
        _ctx("🟢 장내 매수  🔴 장내 매도  🔵 RSU 귀속(보상, 시장 신호 아님)"),
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
        lines.append(f"📋 *{f.institution}*{detail}  _{f.filing_date}_")
    return [
        _sec("*🏛 13F 기관 포지션*\n" + "\n".join(lines)),
    ]


def format_news_block(news: list) -> list:
    relevant = [n for n in news if not n.get("skip") and n.get("summary")]
    if not relevant:
        return []
    blocks = []
    for n in relevant[:5]:
        tag = "🟢" if n.get("sentiment") == "positive" else "🔴" if n.get("sentiment") == "negative" else "⚪"
        blocks.append(_sec(f"*📰 {tag} {n['summary']}*"))
        if n.get("translation"):
            blocks.append(_ctx(n["translation"]))
    return blocks




def format_volume_profile_block(vp: VolumeProfile) -> list:
    poc_emoji = "🔴" if vp.poc_signal == "resistance" else "🟢"
    poc_desc = "매물대 상단 (저항)" if vp.poc_signal == "resistance" else "지지선 확보"
    whale = "  🐋 *Whale Activity Detected*" if vp.whale_detected else ""
    return [
        _sec(f"*📊 수급 구조 (30분 POC)*  {poc_emoji} *${vp.poc_price:.2f}* — {poc_desc}{whale}"),
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
        f"BB 하단 ${sm.bb_lower:.2f} | SMA20 ${sm.sma20:.2f} | BB 상단 ${sm.bb_upper:.2f}  |  "
        f"하단 대비 *{sm.pct_from_lower:+.2f}%*  |  모멘텀 {sm.mom_30m_prev:+.2f}% → {sm.mom_30m_curr:+.2f}%"
    ))

    # ── 베타 초과 이탈 ──
    if sm.beta_excess_pct != 0:
        if sm.beta_excess_pct <= -3:
            beta_line = f"🟢 *β 초과 이탈 {sm.beta_excess_pct:+.2f}%p* — 기대보다 과도한 하락, 통계적 반등 가능"
        elif sm.beta_excess_pct <= -1:
            beta_line = f"🟡 *β 소폭 초과 이탈 {sm.beta_excess_pct:+.2f}%p*"
        elif sm.beta_excess_pct >= 3:
            beta_line = f"🔴 *β 초과 상승 {sm.beta_excess_pct:+.2f}%p* — 기대보다 강한 상승"
        else:
            beta_line = f"⚪ *β 범위 내 {sm.beta_excess_pct:+.2f}%p*"
        blocks.append(_ctx(f"기대수익률 {sm.beta_expected_pct:+.2f}% (β×QQQ)  |  {beta_line}"))

    # ── 피어 분기 경고 (Divergence Warning만 유지, 일반 피어 수치는 상대강도 블록에서 표시) ──
    if sm.divergence_warning:
        blocks.append(_sec(
            f"⚠️ *Divergence Warning*\n"
            f"HOOD 하락 가속 중인데 피어 그룹 반등 중 — 개별 악재 가능성"
        ))
        blocks.append(_ctx(
            f"COIN {sm.peer_coin_pct:+.2f}% / MSTR {sm.peer_mstr_pct:+.2f}% 반등  vs  HOOD 하락 가속"
        ))

    return blocks


def format_dca_attraction_block(sm: Optional[SafetyMargin]) -> list:
    """
    DCA 매력도 점수 1~10을 메시지 최상단용 요약 블록으로 출력.
    SM 없으면 스킵.
    """
    if sm is None:
        return []
    score = sm.dca_attraction
    filled = score
    empty = 10 - filled
    if score >= 8:
        emoji, verdict = "🟢", "DCA 추가매수 매우 우호적"
    elif score >= 6:
        emoji, verdict = "🟡", "DCA 추가매수 고려 가능"
    elif score >= 4:
        emoji, verdict = "⚪", "정기 DCA 유지 권장"
    else:
        emoji, verdict = "🔴", "추가매수 자제 — 리스크 높음"

    bar = "🟦" * filled + "⬜" * empty
    detail_parts = []
    if sm.bb_signal in ("extreme_oversold", "oversold"):
        detail_parts.append(f"BB 이탈({sm.pct_from_lower:+.1f}%)")
    if sm.beta_excess_pct <= -1:
        detail_parts.append(f"β 초과({sm.beta_excess_pct:+.1f}%p)")
    if sm.divergence_warning:
        detail_parts.append("피어분기 경고")

    detail = "  |  ".join(detail_parts) if detail_parts else "복합 지표 기반"

    return [
        _sec(f"*💎 DCA 매력도: {score}/10*  {emoji} {verdict}\n{bar}"),
        _ctx(detail),
        {"type": "divider"},
    ]


def format_dca_block(signal: DCASignal) -> list:
    bar_filled = signal.score // 5
    bar = ("🟩" if signal.score >= 60 else "🟨" if signal.score >= 40 else "🟥") * bar_filled + "⬜" * (20 - bar_filled)
    factors_text = "\n".join(f"• {f}" for f in signal.factors[:3]) if signal.factors else ""
    blocks = [
        _sec(f"*🎯 DCA 시그널: {signal.score}/100*\n{bar}\n*{signal.verdict}*\n{signal.summary}"),
    ]
    if factors_text:
        blocks.append(_ctx(factors_text))
    return blocks


# ─────────────────────────────────────────────
# Slack 전송
# ─────────────────────────────────────────────
def _footer() -> list:
    """메시지 끝 구분선 + 타임스탬프"""
    kst = datetime.now(KST).strftime("%m/%d %H:%M KST")
    return [
        {"type": "divider"},
        _ctx(f"🤖 HOOD Monitor  |  {kst}"),
    ]


def send_slack(blocks: list, text: str = "HOOD Monitor"):
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK_URL not set")
        for b in blocks:
            if isinstance(b.get("text"), dict):
                print(b["text"].get("text", ""))
        return
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text, "blocks": blocks}, timeout=10)
        if resp.status_code != 200:
            log.error(f"Slack error: {resp.status_code} {resp.text}")
        else:
            log.info("Slack sent OK")
    except Exception as e:
        log.error(f"Slack send: {e}")


# ─────────────────────────────────────────────
# 실행 모드
# ─────────────────────────────────────────────
def run_normal():
    """장중 모드: 뉴스 + 기술지표(시그널시) + 내부자 + 가격 급변동"""
    log.info("=== NORMAL ===")
    state = load_state()
    ws = load_weekly_state()
    blocks = []
    today = datetime.now(KST).strftime("%Y-%m-%d")

    if state.get("price_alert_date") != today:
        state["price_alert_max_pct"] = 0
        state["price_alert_direction"] = ""
        state["price_alert_date"] = today

    price = fetch_price()
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
                label = "상승" if direction == "up" else "하락"
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                    f"{emoji} *$HOOD {int(abs_pct)}% {label} 돌파!*\n전일 대비 {abs_pct:.1f}% {label} 중"}})

                # 상대 강도 (β 기반)
                _beta = get_beta()
                if _beta:
                    _qqq = _fetch_ticker_change(BETA_BENCHMARK) or 0.0
                    blocks.extend(format_beta_block(calc_beta_divergence(_beta, _qqq, price.change_pct)))

                # Volume Profile
                vp = analyze_volume_profile(price.current)
                if vp:
                    blocks.extend(format_volume_profile_block(vp))

                state["price_alert_max_pct"] = abs_pct if direction != prev_dir else max(prev_max, abs_pct)
                state["price_alert_direction"] = direction
                ws.setdefault("alerts_fired", []).append(f"주가 {price.change_pct:+.1f}%")

    closes = fetch_price_history(60)
    technicals = TechnicalSignals()
    if closes:
        state["price_history"] = closes[-60:]
        technicals = get_technical_signals(closes)
        if technicals.rsi_alert or technicals.macd_alert:
            blocks.extend(format_technicals_block(technicals))
        ws.setdefault("rsi_readings", []).append(technicals.rsi_14)

    news = fetch_news()
    new_news = [n for n in news if n["hash"] not in state.get("last_news_hashes", [])]
    if new_news:
        new_news = translate_news(new_news)
        blocks.extend(format_news_block(new_news))
        state["last_news_hashes"] = [n["hash"] for n in news[:20]]
        for n in new_news:
            if not n.get("skip") and n.get("summary"):
                ws.setdefault("news_headlines", []).append(n["summary"])

    insider_trades = fetch_insider_trades()
    new_insiders = [t for t in insider_trades
                    if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                    not in state.get("last_insider_hashes", [])]
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))
        state["last_insider_hashes"] = [
            hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
            for t in insider_trades[:30]
        ]
        for t in new_insiders:
            ws.setdefault("insider_trades", []).append(
                f"{t.trade_type}: {t.filer} {t.shares:,}주 "
                + ("대규모" if t.total_value >= 1_000_000 else "중규모" if t.total_value >= 100_000 else "소규모")
            )

    if blocks:
        blocks.insert(0, {"type": "header", "text": {"type": "plain_text",
            "text": f"📊 $HOOD — {datetime.now(KST).strftime('%m/%d %H:%M KST')}"}})
        blocks.extend(_footer())
        send_slack(blocks)
    else:
        log.info("No alerts — quiet")

    save_state(state)
    save_weekly_state(ws)


def run_close():
    """
    장 마감 모드: 종가 확인 + 기술지표 + PCR + 공매도 + DCA
    BUG 1 FIX: 4%+ 이면 state에 pending_morning_alert 저장
    """
    log.info("=== CLOSE ===")
    state = load_state()
    ws = load_weekly_state()
    blocks = []

    price = fetch_price(realtime=False)
    if price and price.prev_close > 0:
        abs_pct = abs(price.change_pct)
        direction = "up" if price.change_pct > 0 else "down"

        # 로깅: 등락률 항상 기록
        log.info(f"종가 등락: {price.change_pct:+.2f}% (prev_close={price.prev_close}, current={price.current})")

        # state 저장 (4%+ 시 morning 알림 예약)
        if abs_pct >= 4:
            state["pending_morning_alert"] = {
                "change_pct": round(price.change_pct, 1),
                "abs_pct": round(abs_pct, 1),
                "direction": direction,
                "date": datetime.now(KST).strftime("%Y-%m-%d"),
            }
            log.info(f"Morning alert queued: {price.change_pct:+.1f}%")
            emoji_big = "🚀" if direction == "up" else "💥"
            label = "상승" if direction == "up" else "하락"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                f"{emoji_big} *$HOOD 종가 {abs_pct:.1f}% {label}* — 내일 08:00 KST 재알림 예정"}})

            # 상대 강도 (β 기반) — 장 마감 후 run_close의 별도 beta 블록과 통합

            # Volume Profile: 장 마감 후엔 1분봉 없으므로 스킵
            # Safety Margin은 closes 계산 후 아래서 처리
        else:
            state["pending_morning_alert"] = None
            emoji_dir = "🌤" if direction == "up" else "🌧"
            mood = "양전 마감" if direction == "up" else "음전 마감"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                f"{emoji_dir} 오늘 종가 {mood}"}})

        # 거래량 context
        if price.volume > 0:
            vol_ratio = round(price.volume / price.vol_avg_5d, 2) if price.vol_avg_5d > 0 else 0
            vol_flag = "  🐋 거래량 폭증" if vol_ratio >= 1.5 else ""
            vol_ctx = f"당일 거래량 {price.volume:,}"
            if price.vol_avg_5d > 0:
                vol_ctx += f" | 5일 평균 {price.vol_avg_5d:,} | *{vol_ratio:.1f}x*{vol_flag}"
            blocks.append(_ctx(vol_ctx))

        state["price_alert_max_pct"] = 0
        state["price_alert_direction"] = ""

    # closes를 price 블록보다 먼저 fetch (Safety Margin에서도 사용)
    closes = fetch_price_history(60)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    blocks.extend(format_technicals_block(technicals))

    # 베타 분석 (캐시 우선, 4%+ 급변동 시에만 이격도 의미 있음)
    beta = get_beta()
    if beta and price and price.prev_close > 0:
        qqq_pct = _fetch_ticker_change(BETA_BENCHMARK) or 0.0
        bd = calc_beta_divergence(beta, qqq_pct, price.change_pct)
        blocks.extend(format_beta_block(bd))

    # Safety Margin: close 모드에서 항상 실행 (4% 조건 제거)
    sm = None
    if price and price.prev_close > 0 and closes:
        sm = check_safety_margin(
            closes,
            price.current,
            actual_pct=price.change_pct,
            beta=get_beta(),
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

    # BTC 상관계수
    btc_corr = calc_btc_correlation()
    if btc_corr:
        blocks.extend(format_btc_correlation_block(btc_corr))

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

    insider_trades = fetch_insider_trades()
    log.info(f"내부자 거래 fetch 결과: 총 {len(insider_trades)}건")
    new_insiders = [t for t in insider_trades
                    if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                    not in state.get("last_insider_hashes", [])]
    log.info(f"내부자 거래 신규: {len(new_insiders)}건 (중복 제외)")
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))
        state["last_insider_hashes"] = [
            hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
            for t in insider_trades[:30]
        ]
        for t in new_insiders:
            ws.setdefault("insider_trades", []).append(
                f"{t.trade_type}: {t.filer} {t.shares:,}주"
                + (f" @ ${t.price:.2f}" if t.price > 0 else "")
            )
    news = fetch_news()
    news = translate_news(news)
    news_blocks = format_news_block(news)
    if news_blocks:
        blocks.extend(news_blocks)
        log.info(f"뉴스 블록 추가: {len(news_blocks)}개")
    else:
        log.info("표시할 관련 뉴스 없음")

    dca = calculate_dca_signal(price or PriceData(), technicals, options, short, insider_trades, news, sm=sm)
    blocks.extend(format_dca_block(dca))

    blocks.insert(0, {"type": "header", "text": {"type": "plain_text",
        "text": f"🔔 $HOOD 장 마감 — {datetime.now(KST).strftime('%m/%d')}"}})
    blocks.extend(_footer())
    send_slack(blocks)

    save_state(state)
    save_weekly_state(ws)
    log.info("Close done")


def run_morning():
    """
    08:00 KST 아침 알림
    BUG 1 FIX: fetch_price() 직접 호출 대신 state의 pending_morning_alert 읽음
    """
    log.info("=== MORNING ===")
    state = load_state()

    alert = state.get("pending_morning_alert")
    if not alert:
        log.info("No pending morning alert — silent")
        return

    abs_pct = alert["abs_pct"]
    direction = alert["direction"]
    emoji = "🚀" if direction == "up" else "💥"
    label = "상승" if direction == "up" else "하락"

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"☀️ $HOOD 아침 브리핑 — {datetime.now(KST).strftime('%m/%d')}"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"{emoji} *어제 종가 기준 {abs_pct:.1f}% {label}*"}},
    ]
    blocks.extend(_footer())
    send_slack(blocks)

    # 발송 후 초기화
    state["pending_morning_alert"] = None
    save_state(state)
    log.info(f"Morning alert sent: {alert['change_pct']:+.1f}%")


def run_13f():
    """13F 기관 포지션 (주 1회 토요일)"""
    log.info("=== 13F ===")
    state = load_state()
    filings = fetch_13f_filings()
    new_filings = [f for f in filings
                   if hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
                   not in state.get("last_13f_hashes", [])]

    if new_filings:
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "🏛 $HOOD 13F 기관 포지션 업데이트"}},
            {"type": "divider"},
        ]
        blocks.extend(format_13f_block(new_filings))
        blocks.extend(_footer())
        send_slack(blocks)

        state["last_13f_hashes"] = [
            hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
            for f in filings[:30]
        ]
        save_state(state)

    log.info(f"13F done — {len(new_filings)} new")


def run_weekly():
    """
    주간 브리핑 (매주 월 08:00 KST)
    BUG 4 FIX: 주가 숫자($) 완전 제거, DCA verdict 강조
    """
    log.info("=== WEEKLY ===")
    ws = load_weekly_state()
    closes = fetch_price_history(60)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    options = fetch_options_pcr()
    short = fetch_short_interest()
    insider_trades = fetch_insider_trades()
    news = fetch_news()
    news = translate_news(news)

    # 주간 변동률 (가격 숫자 없이 %)
    price = fetch_price()
    weekly_change_str = ""
    if price:
        weekly_change_str = f"이번 주 마감 기준 {price.change_pct:+.1f}% ({('상승' if price.change_pct >= 0 else '하락')})"

    dca = calculate_dca_signal(price or PriceData(), technicals, options, short, insider_trades, news)
    alerts = ws.get("alerts_fired", [])
    insider_summary = ws.get("insider_trades", [])
    news_summary = ws.get("news_headlines", [])
    rsi_readings = ws.get("rsi_readings", [])
    pcr_readings = ws.get("pcr_readings", [])
    short_readings = ws.get("short_readings", [])

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"📋 $HOOD 주간 브리핑 — {datetime.now(KST).strftime('%m/%d')} 월"}},
        {"type": "divider"},
    ]

    # 주간 변동 요약 (% 만, 가격 숫자 없음)
    if weekly_change_str:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*📅 주간 등락*\n{weekly_change_str}"}})

    blocks.extend(format_technicals_block(technicals))

    if pcr_readings:
        avg_pcr = sum(pcr_readings) / len(pcr_readings)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*📈 주간 PCR 평균: {avg_pcr:.3f}*" + (f" (현재 {options.pcr:.3f})" if options else "")}})

    if short_readings:
        avg_short = sum(short_readings) / len(short_readings)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"*🩳 주간 공매도 평균: {avg_short:.1f}%*" + (f" (최신 {short.short_pct:.1f}%)" if short else "")}})

    if alerts:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*🚨 주간 알림*\n" + "\n".join(f"• {a}" for a in alerts[-8:])}})

    if insider_summary:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*🕴 주간 내부자 거래*\n" + "\n".join(f"• {t}" for t in insider_summary[-5:])}})

    if news_summary:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            "*📰 주간 주요 뉴스*\n" + "\n".join(f"• {h}" for h in news_summary[-5:])}})

    blocks.append({"type": "divider"})
    blocks.extend(format_dca_block(dca))
    blocks.extend(_footer())

    send_slack(blocks)

    save_weekly_state({
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "alerts_fired": [], "insider_trades": [], "news_headlines": [],
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
        f"*💼 $HOOD DCA 포지션 현황*",
        f"보유 수량: *{shares:,.1f}주*",
        f"평균 매수가: *${avg_price:.2f}*",
    ]

    if history:
        lines.append(f"\n*📋 매수 이력 (최근 5건)*")
        for h in history[-5:]:
            lines.append(f"• {h['date']} — {h['shares']:.1f}주 @ ${h['price']:.2f}")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "💼 $HOOD DCA 현황"}},
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
    log.info(f"HOOD Monitor v3.1 — mode: {mode} | {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}")
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
