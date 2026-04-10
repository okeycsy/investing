#!/usr/bin/env python3
"""
S&P 500 Market Scanner v3.0
================================
v2.0: yfinance 배치 다운로드 방식으로 전환 (429 해결)
v2.1: CMF·EvsR·ADX·BB Squeeze 고도화 (Prop-desk 지표)
v3.0: S&P 500 전체 확대 (~490종목) + --ticker 단일 종목 모드
"""

import os
import sys
import time
import json
import logging
import argparse
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional
from dataclasses import dataclass, field

try:
    import yfinance as yf
except ImportError:
    print("yfinance 없음 — pip install yfinance")
    sys.exit(1)

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
SLACK_WEBHOOK = os.environ.get("MARKET_SCAN_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("market_scan")

# ─────────────────────────────────────────────
# S&P 500 종목 + GICS 섹터 (2025 Q1 기준)
# 편입/제외 변경 시 해당 종목만 수정
# ─────────────────────────────────────────────
SP500 = {
    # ── Information Technology ────────────────────────────────────────────
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "AVGO": "Technology", "ORCL": "Technology", "CRM":  "Technology",
    "ACN":  "Technology", "CSCO": "Technology", "IBM":  "Technology",
    "QCOM": "Technology", "AMD":  "Technology", "INTU": "Technology",
    "TXN":  "Technology", "AMAT": "Technology", "LRCX": "Technology",
    "ADI":  "Technology", "MU":   "Technology", "KLAC": "Technology",
    "SNPS": "Technology", "CDNS": "Technology", "PANW": "Technology",
    "CRWD": "Technology", "MRVL": "Technology", "ON":   "Technology",
    "NXPI": "Technology", "MCHP": "Technology", "FTNT": "Technology",
    "STX":  "Technology", "FSLR": "Technology", "KEYS": "Technology",
    "ANSS": "Technology", "TER":  "Technology", "ENPH": "Technology",
    "GLW":  "Technology", "HPQ":  "Technology", "HPE":  "Technology",
    "IT":   "Technology", "CTSH": "Technology", "CDW":  "Technology",
    "MPWR": "Technology", "SWKS": "Technology", "TRMB": "Technology",
    "GEN":  "Technology", "PTC":  "Technology", "ZBRA": "Technology",
    "EPAM": "Technology", "VRSN": "Technology", "WDC":  "Technology",
    "NTAP": "Technology", "NOW":  "Technology", "DDOG": "Technology",
    "SNOW": "Technology", "NET":  "Technology", "TEAM": "Technology",
    "WDAY": "Technology", "ADBE": "Technology", "ZS":   "Technology",
    "MDB":  "Technology", "HUBS": "Technology", "ADP":  "Technology",
    "INTC": "Technology", "ASML": "Technology", "TSM":  "Technology",
    "JNPR": "Technology", "FFIV": "Technology", "AKAM": "Technology",
    "GDDY": "Technology", "PAYC": "Technology", "OKTA": "Technology",
    "APP":  "Technology", "PLTR": "Technology",
    # ── Communication Services ────────────────────────────────────────────
    "META":  "Comm Services", "GOOGL": "Comm Services", "GOOG":  "Comm Services",
    "NFLX":  "Comm Services", "TMUS":  "Comm Services", "CMCSA": "Comm Services",
    "VZ":    "Comm Services", "T":     "Comm Services", "DIS":   "Comm Services",
    "CHTR":  "Comm Services", "WBD":   "Comm Services", "FOXA":  "Comm Services",
    "FOX":   "Comm Services", "OMC":   "Comm Services", "IPG":   "Comm Services",
    "LYV":   "Comm Services", "EA":    "Comm Services", "TTWO":  "Comm Services",
    "MTCH":  "Comm Services",
    # ── Consumer Discretionary ────────────────────────────────────────────
    "AMZN": "Cons Discretionary", "TSLA": "Cons Discretionary",
    "MCD":  "Cons Discretionary", "NKE":  "Cons Discretionary",
    "HD":   "Cons Discretionary", "LOW":  "Cons Discretionary",
    "BKNG": "Cons Discretionary", "TJX":  "Cons Discretionary",
    "SBUX": "Cons Discretionary", "CMG":  "Cons Discretionary",
    "ABNB": "Cons Discretionary", "MAR":  "Cons Discretionary",
    "HLT":  "Cons Discretionary", "YUM":  "Cons Discretionary",
    "DHI":  "Cons Discretionary", "LEN":  "Cons Discretionary",
    "PHM":  "Cons Discretionary", "NVR":  "Cons Discretionary",
    "RCL":  "Cons Discretionary", "CCL":  "Cons Discretionary",
    "NCLH": "Cons Discretionary", "MGM":  "Cons Discretionary",
    "WYNN": "Cons Discretionary", "LVS":  "Cons Discretionary",
    "CZR":  "Cons Discretionary", "F":    "Cons Discretionary",
    "GM":   "Cons Discretionary", "ROST": "Cons Discretionary",
    "ORLY": "Cons Discretionary", "AZO":  "Cons Discretionary",
    "GPC":  "Cons Discretionary", "KMX":  "Cons Discretionary",
    "AN":   "Cons Discretionary", "APTV": "Cons Discretionary",
    "TSCO": "Cons Discretionary", "DRI":  "Cons Discretionary",
    "RL":   "Cons Discretionary", "TPR":  "Cons Discretionary",
    "LKQ":  "Cons Discretionary", "BWA":  "Cons Discretionary",
    "BBY":  "Cons Discretionary", "ETSY": "Cons Discretionary",
    "EBAY": "Cons Discretionary", "EXPE": "Cons Discretionary",
    "ULTA": "Cons Discretionary", "LULU": "Cons Discretionary",
    "POOL": "Cons Discretionary", "WH":   "Cons Discretionary",
    "H":    "Cons Discretionary", "MHK":  "Cons Discretionary",
    "GNTX": "Cons Discretionary",
    # ── Consumer Staples ──────────────────────────────────────────────────
    "WMT":  "Cons Staples", "COST": "Cons Staples", "PG":   "Cons Staples",
    "KO":   "Cons Staples", "PEP":  "Cons Staples", "PM":   "Cons Staples",
    "MO":   "Cons Staples", "MDLZ": "Cons Staples", "CL":   "Cons Staples",
    "KMB":  "Cons Staples", "CHD":  "Cons Staples", "SJM":  "Cons Staples",
    "CAG":  "Cons Staples", "HRL":  "Cons Staples", "CPB":  "Cons Staples",
    "MKC":  "Cons Staples", "K":    "Cons Staples", "GIS":  "Cons Staples",
    "HSY":  "Cons Staples", "STZ":  "Cons Staples", "TAP":  "Cons Staples",
    "MNST": "Cons Staples", "KHC":  "Cons Staples", "WBA":  "Cons Staples",
    "SYY":  "Cons Staples", "ADM":  "Cons Staples", "BG":   "Cons Staples",
    "EL":   "Cons Staples",
    # ── Healthcare ────────────────────────────────────────────────────────
    "JNJ":  "Healthcare", "UNH":  "Healthcare", "PFE":  "Healthcare",
    "ABBV": "Healthcare", "MRK":  "Healthcare", "TMO":  "Healthcare",
    "ABT":  "Healthcare", "DHR":  "Healthcare", "BMY":  "Healthcare",
    "AMGN": "Healthcare", "LLY":  "Healthcare", "SYK":  "Healthcare",
    "MDT":  "Healthcare", "BSX":  "Healthcare", "EW":   "Healthcare",
    "BDX":  "Healthcare", "ISRG": "Healthcare", "VRTX": "Healthcare",
    "REGN": "Healthcare", "GILD": "Healthcare", "BIIB": "Healthcare",
    "MRNA": "Healthcare", "IDXX": "Healthcare", "DXCM": "Healthcare",
    "GEHC": "Healthcare", "HCA":  "Healthcare", "CI":   "Healthcare",
    "CVS":  "Healthcare", "MCK":  "Healthcare", "CAH":  "Healthcare",
    "ABC":  "Healthcare", "IQV":  "Healthcare", "ZBH":  "Healthcare",
    "HOLX": "Healthcare", "MTD":  "Healthcare", "WAT":  "Healthcare",
    "TFX":  "Healthcare", "BAX":  "Healthcare", "STE":  "Healthcare",
    "HSIC": "Healthcare", "CNC":  "Healthcare", "MOH":  "Healthcare",
    "HUM":  "Healthcare", "VTRS": "Healthcare", "INCY": "Healthcare",
    "ALNY": "Healthcare", "RMD":  "Healthcare", "PODD": "Healthcare",
    "COO":  "Healthcare", "ALGN": "Healthcare", "DVA":  "Healthcare",
    "LH":   "Healthcare", "DGX":  "Healthcare", "BIO":  "Healthcare",
    "TECH": "Healthcare", "SOLV": "Healthcare",
    # ── Financials ────────────────────────────────────────────────────────
    "JPM":  "Financials", "BAC":  "Financials", "WFC":  "Financials",
    "MS":   "Financials", "GS":   "Financials", "BLK":  "Financials",
    "C":    "Financials", "AXP":  "Financials", "SCHW": "Financials",
    "USB":  "Financials", "PNC":  "Financials", "TFC":  "Financials",
    "COF":  "Financials", "MTB":  "Financials", "RF":   "Financials",
    "HBAN": "Financials", "CFG":  "Financials", "FITB": "Financials",
    "KEY":  "Financials", "BK":   "Financials", "STT":  "Financials",
    "NTRS": "Financials", "CB":   "Financials", "MET":  "Financials",
    "PRU":  "Financials", "AFL":  "Financials", "ALL":  "Financials",
    "AIG":  "Financials", "HIG":  "Financials", "TRV":  "Financials",
    "PGR":  "Financials", "CINF": "Financials", "WRB":  "Financials",
    "SPGI": "Financials", "MCO":  "Financials", "ICE":  "Financials",
    "CME":  "Financials", "NDAQ": "Financials", "FIS":  "Financials",
    "FISV": "Financials", "FI":   "Financials", "GPN":  "Financials",
    "V":    "Financials", "MA":   "Financials", "PYPL": "Financials",
    "COIN": "Financials", "AMP":  "Financials", "RJF":  "Financials",
    "BEN":  "Financials", "IVZ":  "Financials", "TROW": "Financials",
    "HOOD": "Financials", "MKTX": "Financials", "BR":   "Financials",
    "KKR":  "Financials", "BX":   "Financials", "APO":  "Financials",
    "ARES": "Financials", "OWL":  "Financials",
    # ── Industrials ───────────────────────────────────────────────────────
    "HON":  "Industrials", "GE":   "Industrials", "CAT":  "Industrials",
    "DE":   "Industrials", "EMR":  "Industrials", "ETN":  "Industrials",
    "PH":   "Industrials", "ROK":  "Industrials", "DOV":  "Industrials",
    "ITW":  "Industrials", "MMM":  "Industrials", "FTV":  "Industrials",
    "XYL":  "Industrials", "HUBB": "Industrials", "AME":  "Industrials",
    "GNRC": "Industrials", "CARR": "Industrials", "OTIS": "Industrials",
    "CTAS": "Industrials", "PAYX": "Industrials", "FAST": "Industrials",
    "ODFL": "Industrials", "VRSK": "Industrials", "CPRT": "Industrials",
    "EXPD": "Industrials", "CHRW": "Industrials", "FDX":  "Industrials",
    "UPS":  "Industrials", "NSC":  "Industrials", "CSX":  "Industrials",
    "UNP":  "Industrials", "WAB":  "Industrials", "JBHT": "Industrials",
    "GWW":  "Industrials", "MAS":  "Industrials", "SWK":  "Industrials",
    "SNA":  "Industrials", "IR":   "Industrials", "TT":   "Industrials",
    "LII":  "Industrials", "ALLE": "Industrials", "NDSN": "Industrials",
    "ROP":  "Industrials", "CSL":  "Industrials", "AXON": "Industrials",
    "LMT":  "Industrials", "GD":   "Industrials", "RTX":  "Industrials",
    "NOC":  "Industrials", "BA":   "Industrials", "HII":  "Industrials",
    "TDG":  "Industrials", "TXT":  "Industrials", "HEI":  "Industrials",
    "SAIC": "Industrials", "LDOS": "Industrials", "BAH":  "Industrials",
    "CACI": "Industrials",
    # ── Materials ─────────────────────────────────────────────────────────
    "LIN":  "Materials", "APD":  "Materials", "SHW":  "Materials",
    "ECL":  "Materials", "DD":   "Materials", "NEM":  "Materials",
    "FCX":  "Materials", "NUE":  "Materials", "STLD": "Materials",
    "RS":   "Materials", "BALL": "Materials", "IP":   "Materials",
    "PKG":  "Materials", "AMCR": "Materials", "EMN":  "Materials",
    "FMC":  "Materials", "MOS":  "Materials", "IFF":  "Materials",
    "CE":   "Materials", "ALB":  "Materials", "RPM":  "Materials",
    "PPG":  "Materials", "SEE":  "Materials", "AVY":  "Materials",
    "CF":   "Materials",
    # ── Real Estate ───────────────────────────────────────────────────────
    "AMT":  "Real Estate", "PLD":  "Real Estate", "EQIX": "Real Estate",
    "CCI":  "Real Estate", "SBAC": "Real Estate", "DLR":  "Real Estate",
    "WELL": "Real Estate", "PSA":  "Real Estate", "EQR":  "Real Estate",
    "AVB":  "Real Estate", "CPT":  "Real Estate", "ESS":  "Real Estate",
    "UDR":  "Real Estate", "INVH": "Real Estate", "MAA":  "Real Estate",
    "NNN":  "Real Estate", "O":    "Real Estate", "SPG":  "Real Estate",
    "BXP":  "Real Estate", "ARE":  "Real Estate", "VTR":  "Real Estate",
    "VICI": "Real Estate", "IRM":  "Real Estate", "EXR":  "Real Estate",
    "CUBE": "Real Estate", "FR":   "Real Estate", "WY":   "Real Estate",
    "HST":  "Real Estate", "KIM":  "Real Estate", "REG":  "Real Estate",
    # ── Utilities ─────────────────────────────────────────────────────────
    "NEE":  "Utilities", "DUK":  "Utilities", "SO":   "Utilities",
    "D":    "Utilities", "AEP":  "Utilities", "EXC":  "Utilities",
    "XEL":  "Utilities", "ES":   "Utilities", "ETR":  "Utilities",
    "FE":   "Utilities", "PPL":  "Utilities", "CMS":  "Utilities",
    "NI":   "Utilities", "ATO":  "Utilities", "LNT":  "Utilities",
    "WEC":  "Utilities", "EVRG": "Utilities", "SRE":  "Utilities",
    "PCG":  "Utilities", "ED":   "Utilities", "CEG":  "Utilities",
    "NRG":  "Utilities", "VST":  "Utilities", "EIX":  "Utilities",
    "AWK":  "Utilities", "AES":  "Utilities",
    # ── Energy ────────────────────────────────────────────────────────────
    "XOM":  "Energy", "CVX":  "Energy", "COP":  "Energy",
    "EOG":  "Energy", "DVN":  "Energy", "FANG": "Energy",
    "OXY":  "Energy", "HES":  "Energy", "MRO":  "Energy",
    "APA":  "Energy", "EQT":  "Energy", "AR":   "Energy",
    "CTRA": "Energy", "SLB":  "Energy", "HAL":  "Energy",
    "BKR":  "Energy", "MPC":  "Energy", "VLO":  "Energy",
    "PSX":  "Energy", "OKE":  "Energy", "WMB":  "Energy",
    "KMI":  "Energy", "LNG":  "Energy", "TRGP": "Energy",
    "DINO": "Energy",
}



# ─────────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────────
@dataclass
class TickerScore:
    ticker:      str   = ""
    sector:      str   = ""
    score:       int   = 0
    raw:         int   = 0
    grade:       str   = ""
    grade_emoji: str   = ""
    rsi:         float = 50.0
    mfi:         float = 50.0
    cmf:         float = 0.0    # Chaikin Money Flow
    adx:         float = 0.0    # ADX 추세 강도
    evsr:        float = 0.0    # Effort vs Result (흡수 비율)
    upvol:       float = 1.0    # 상승일/하락일 거래량 비율
    squeeze:     bool  = False  # BB Squeeze 발동 여부
    layers:      dict  = field(default_factory=dict)
    error:       bool  = False


# ─────────────────────────────────────────────
# 배치 OHLCV 다운로드 (핵심: 전종목 한번에)
# ─────────────────────────────────────────────
def batch_download(tickers: list, period: str = "6mo") -> dict:
    """
    yfinance 배치 다운로드 — 전종목을 청크(30종목)로 나눠 요청.
    반환: {ticker: {"closes":[], "highs":[], "lows":[], "volumes":[]}}
    """
    result = {}
    chunk_size = 25  # 500종목 대응: 25종목씩 (안정성 우선)

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i: i + chunk_size]
        log.info(f"배치 다운로드 [{i+1}~{i+len(chunk)}/{len(tickers)}]: {' '.join(chunk[:5])}...")

        try:
            df = yf.download(
                chunk,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            log.error(f"배치 다운로드 실패: {e}")
            continue

        # 단일 종목이면 컬럼 구조가 다름
        if len(chunk) == 1:
            t = chunk[0]
            try:
                closes  = df["Close"].dropna().tolist()
                highs   = df["High"].dropna().tolist()
                lows    = df["Low"].dropna().tolist()
                volumes = [int(v) for v in df["Volume"].dropna().tolist()]
                n = min(len(closes), len(highs), len(lows), len(volumes))
                if n >= 30:
                    result[t] = {
                        "closes": closes[-n:], "highs": highs[-n:],
                        "lows": lows[-n:],   "volumes": volumes[-n:],
                    }
            except Exception as e:
                log.warning(f"{t} 파싱 실패: {e}")
            continue

        # 복수 종목
        for t in chunk:
            try:
                if t not in df.columns.get_level_values(0):
                    log.warning(f"{t}: 데이터 없음")
                    continue
                sub = df[t].dropna()
                closes  = sub["Close"].tolist()
                highs   = sub["High"].tolist()
                lows    = sub["Low"].tolist()
                volumes = [int(v) for v in sub["Volume"].tolist()]
                n = min(len(closes), len(highs), len(lows), len(volumes))
                if n < 30:
                    log.warning(f"{t}: 데이터 부족 ({n}일)")
                    continue
                result[t] = {
                    "closes": closes[-n:], "highs": highs[-n:],
                    "lows": lows[-n:],   "volumes": volumes[-n:],
                }
                log.info(f"  {t}: {n}일 데이터 OK")
            except Exception as e:
                log.warning(f"{t} 파싱 실패: {e}")

        # 청크 간 딜레이 (부드럽게)
        if i + chunk_size < len(tickers):
            time.sleep(3)

    return result


# ─────────────────────────────────────────────
# 지표 계산 (순수 파이썬, 의존성 없음)
# ─────────────────────────────────────────────
def _rsi(closes: list, period: int = 14) -> float:
    """RSI — Wilder smoothing (Wilder 원래 공식, 단순평균 버그 수정)"""
    if len(closes) < period + 1:
        return 50.0
    diffs = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [d if d > 0 else 0.0 for d in diffs]
    losses = [-d if d < 0 else 0.0 for d in diffs]
    # 씨드: 첫 period 봉 단순평균
    avg_g = sum(gains[:period])  / period
    avg_l = sum(losses[:period]) / period
    # Wilder 지수평활
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 2)


def _macd_hist(closes: list) -> tuple:
    """(현재 히스토그램, 전봉 히스토그램)"""
    def ema(data, p):
        if len(data) < p:
            return [0.0] * len(data)
        k = 2 / (p+1)
        r = [sum(data[:p]) / p]
        for v in data[p:]:
            r.append(v * k + r[-1] * (1-k))
        return r
    if len(closes) < 35:
        return 0.0, 0.0
    e12 = ema(closes, 12); e26 = ema(closes, 26)
    n = min(len(e12), len(e26))
    macd = [e12[i] - e26[i] for i in range(n)]
    sig  = ema(macd, 9)
    cur  = macd[-1] - sig[-1]
    # 전봉: closes[:-1]로 재계산
    if len(closes) > 35:
        e12p = ema(closes[:-1], 12); e26p = ema(closes[:-1], 26)
        np2  = min(len(e12p), len(e26p))
        macdp = [e12p[i] - e26p[i] for i in range(np2)]
        sigp  = ema(macdp, 9)
        prev  = macdp[-1] - sigp[-1]
    else:
        prev = cur
    return round(cur, 6), round(prev, 6)


def _ema(data: list, period: int) -> list:
    if len(data) < period:
        return []
    k = 2 / (period+1)
    r = [sum(data[:period]) / period]
    for v in data[period:]:
        r.append(v * k + r[-1] * (1-k))
    return r


def _cmf(highs, lows, closes, volumes, period: int = 21) -> float:
    """
    Chaikin Money Flow — OBV 대체.
    종가가 당일 고저 중 어디에 위치하는지 반영 → 시가총액 편향 없음.
    +0.05 이상 = 매수 압력, -0.05 이하 = 매도 압력
    """
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period:
        return 0.0
    clv_vol = vol_sum = 0.0
    for i in range(n - period, n):
        hl = highs[i] - lows[i]
        clv = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / hl if hl else 0.0
        clv_vol += clv * volumes[i]
        vol_sum  += volumes[i]
    return round(clv_vol / vol_sum, 4) if vol_sum else 0.0


def _evsr_absorption(highs, lows, closes, volumes, period: int = 20) -> float:
    """
    Effort vs Result (EvsR) Absorption — Whale 감지 대체.
    거래량 노력(Effort) 대비 가격 결과(Result)가 기대 이하 = 숨겨진 매집(Absorption).
    반환값 > 1.5: 흡수 가능성, > 2.0: 강한 흡수 신호
    """
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 1:
        return 0.0
    atr_v = _atr(highs, lows, closes, period)
    if not atr_v or atr_v == 0:
        return 0.0
    avg_vol = sum(volumes[-period:]) / period
    if avg_vol == 0:
        return 0.0
    vol_effort   = volumes[-1] / avg_vol                      # 상대 거래량
    price_result = abs(closes[-1] - closes[-2]) / atr_v      # 상대 가격 변화
    if price_result == 0:
        return min(vol_effort * 2, 5.0)                       # 거래량 있는데 가격 불변 = 최대 흡수
    return round(vol_effort / price_result, 2)



def _mfi(highs, lows, closes, volumes, period=14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 1:
        return None
    tp = [(highs[i]+lows[i]+closes[i])/3 for i in range(n)]
    pos = neg = 0.0
    for i in range(n-period, n):
        mf = tp[i] * volumes[i]
        if tp[i] > tp[i-1]: pos += mf
        else:                neg += mf
    return round(100 - 100/(1 + pos/neg), 2) if neg else 100.0


def _stoch(highs, lows, closes, period=14, sk=3, sd=3) -> tuple:
    n = min(len(highs), len(lows), len(closes))
    if n < period + sk + sd:
        return None, None
    raw_k = []
    for i in range(period-1, n):
        hh = max(highs[i-period+1: i+1])
        ll = min(lows[i-period+1: i+1])
        raw_k.append((closes[i]-ll)/(hh-ll)*100 if hh != ll else 50.0)
    ks = [sum(raw_k[i-sk+1: i+1])/sk for i in range(sk-1, len(raw_k))]
    if len(ks) < sd:
        return None, None
    return round(ks[-1], 2), round(sum(ks[-sd:])/sd, 2)


def _atr(highs, lows, closes, period=14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    trs = [max(highs[i]-lows[i],
               abs(highs[i]-closes[i-1]),
               abs(lows[i]-closes[i-1])) for i in range(1, n)]
    return sum(trs[-period:]) / period if len(trs) >= period else None


def _adx(highs, lows, closes, period: int = 14) -> tuple:
    """
    ADX(14) — 추세 강도.
    반환: (adx, plus_di, minus_di)
    ADX < 20: 횡보, ADX > 25: 추세 확립
    매수 컨텍스트: ADX > 20 + -DI 하락 = 하락추세 바닥 후보
    """
    n = min(len(highs), len(lows), len(closes))
    if n < period * 2 + 1:
        return 0.0, 0.0, 0.0

    trs, pdms, ndms = [], [], []
    for i in range(1, n):
        tr  = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        up  = highs[i]   - highs[i-1]
        dn  = lows[i-1]  - lows[i]
        pdms.append(up if up > dn and up > 0 else 0.0)
        ndms.append(dn if dn > up and dn > 0 else 0.0)
        trs.append(tr)

    def _wilder(data, p):
        if len(data) < p:
            return []
        r = [sum(data[:p])]
        for v in data[p:]:
            r.append(r[-1] - r[-1] / p + v)
        return r

    atr_s = _wilder(trs,  period)
    pdi_s = _wilder(pdms, period)
    ndi_s = _wilder(ndms, period)
    if not atr_s:
        return 0.0, 0.0, 0.0

    dx_series = []
    for i in range(len(atr_s)):
        if atr_s[i] == 0:
            continue
        pdi = 100 * pdi_s[i] / atr_s[i]
        ndi = 100 * ndi_s[i] / atr_s[i]
        di_sum = pdi + ndi
        dx_series.append(100 * abs(pdi - ndi) / di_sum if di_sum else 0.0)

    if len(dx_series) < period:
        return 0.0, 0.0, 0.0
    adx = sum(dx_series[-period:]) / period
    pdi_last = 100 * pdi_s[-1] / atr_s[-1] if atr_s[-1] else 0.0
    ndi_last = 100 * ndi_s[-1] / atr_s[-1] if atr_s[-1] else 0.0
    return round(adx, 2), round(pdi_last, 2), round(ndi_last, 2)


def _bb_squeeze(closes, highs, lows, period: int = 20,
                bb_mult: float = 2.0, kc_mult: float = 1.5) -> dict:
    """
    Bollinger Band Squeeze (TTM Squeeze 원리).
    BB폭 < Keltner Channel폭 → Squeeze 발동 = 폭발 직전 눌림.
    반환: {"squeeze": bool, "bb_pos": float, "below_lower": bool}
    bb_pos: -1(하단) ~ +1(상단) / below_lower: BB 하단 이탈 여부
    """
    n = min(len(closes), len(highs), len(lows))
    if n < period:
        return {"squeeze": False, "bb_pos": 0.0, "below_lower": False}

    sma = sum(closes[-period:]) / period
    std = (sum((x - sma) ** 2 for x in closes[-period:]) / period) ** 0.5
    bb_upper = sma + bb_mult * std
    bb_lower = sma - bb_mult * std
    bb_width = bb_upper - bb_lower

    atr_v = _atr(highs, lows, closes, period)
    kc_width = 2 * kc_mult * atr_v if atr_v else bb_width + 1  # ATR 없으면 Squeeze 아님

    squeeze      = bb_width < kc_width
    below_lower  = closes[-1] < bb_lower
    near_lower   = closes[-1] < bb_lower * 1.03
    bb_pos       = (closes[-1] - bb_lower) / bb_width - 0.5 if bb_width else 0.0
    bb_pos       = round(max(-0.5, min(0.5, bb_pos)) * 2, 2)  # -1 ~ +1

    return {"squeeze": squeeze, "bb_pos": bb_pos,
            "below_lower": below_lower, "near_lower": near_lower}


def _rsi_divergence(closes: list, lookback: int = 20) -> bool:
    if len(closes) < lookback + 14:
        return False
    window = closes[-(lookback+14):]
    rsi_s  = [_rsi(window[:i+1]) for i in range(14, len(window))]
    if len(rsi_s) < lookback:
        return False
    pw = closes[-lookback:]; rw = rsi_s[-lookback:]
    mid = len(pw) // 2
    return min(pw[mid:]) < min(pw[:mid]) and min(rw[mid:]) > min(rw[:mid])


def _cmf_turning(highs, lows, closes, volumes,
                 period: int = 21, lookback: int = 5) -> dict:
    """
    CMF 방향 전환 감지.
    현재 CMF vs {lookback}일 전 CMF를 비교해 음→양 전환 중인지 확인.
    반환: {"now": float, "prev": float, "turning": bool, "positive": bool}
    turning = True: 음수에서 양수 방향으로 전환 중 (가장 강한 신호)
    """
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + lookback:
        return {"now": 0.0, "prev": 0.0, "turning": False, "positive": False}

    def _cmf_at(idx_end):
        clv_vol = vol_sum = 0.0
        for i in range(idx_end - period, idx_end):
            hl = highs[i] - lows[i]
            clv = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / hl if hl else 0.0
            clv_vol += clv * volumes[i]
            vol_sum  += volumes[i]
        return clv_vol / vol_sum if vol_sum else 0.0

    now  = round(_cmf_at(n), 4)
    prev = round(_cmf_at(n - lookback), 4)
    turning  = prev < -0.02 and now > prev          # 음수에서 올라오는 중
    positive = now > 0.02                            # 이미 양수 전환 완료
    return {"now": now, "prev": prev, "turning": turning, "positive": positive}


def _upvol_ratio(closes: list, volumes: list, period: int = 10) -> float:
    """
    상승일 거래량 vs 하락일 거래량 비율 (최근 period일).
    > 1.5: 상승일에 거래량 집중 = 매수세 우위
    > 2.0: 강한 매수세 (바닥 반등 초입 핵심 신호)
    """
    n = min(len(closes), len(volumes))
    if n < period + 1:
        return 1.0
    up_vol = dn_vol = 0
    for i in range(n - period, n):
        if closes[i] > closes[i-1]:
            up_vol += volumes[i]
        elif closes[i] < closes[i-1]:
            dn_vol += volumes[i]
    return round(up_vol / dn_vol, 2) if dn_vol else min(up_vol / 1, 5.0)


def _ema20_reclaim(closes: list) -> dict:
    """
    EMA20 회복 상태 감지.
    반환: {"above": bool, "just_crossed": bool, "distance_pct": float}
    just_crossed: 최근 5일 내 EMA20 아래→위 돌파
    distance_pct: (현재가 - EMA20) / EMA20 × 100
    """
    e20 = _ema(closes, 20)
    if not e20 or len(e20) < 6:
        return {"above": False, "just_crossed": False, "distance_pct": 0.0}
    cur      = closes[-1]
    ema_now  = e20[-1]
    above    = cur > ema_now
    dist_pct = round((cur - ema_now) / ema_now * 100, 2)

    # 최근 5봉 중 EMA 아래 → 위 전환이 있었나
    just_crossed = False
    window_c = closes[-(5+1):]
    window_e = e20[-(5+1):]
    for i in range(1, len(window_c)):
        if window_c[i-1] < window_e[i-1] and window_c[i] >= window_e[i]:
            just_crossed = True
            break
    return {"above": above, "just_crossed": just_crossed, "distance_pct": dist_pct}


# ─────────────────────────────────────────────
# 단일 레이어 스코어 v5 — 세부 백테스트 기반 최종 최적화
#
# 세부 백테스트 결과 (HOOD 2년, Layer A 분해):
#   CMF   ρ=+0.259 ✅  단일 최강 예측 지표 → 가중치 최대화
#   EvsR  ρ=+0.102 ✅  유효한 보조 지표 → 유지
#   UpVol ρ=-0.052 ❌  역방향 → 제거
#   B레이어 A↑+B↓ > A↑+B↑ ❌ B가 신호 희석 → 제거
#
#   CMF(50pt) + EvsR(30pt) = 80pt → 100점 정규화
#   모든 다른 지표/함수는 보존 — 향후 재검증 예정
# ─────────────────────────────────────────────
def score_ticker(ticker: str, sector: str, ohlcv: dict) -> TickerScore:
    ts = TickerScore(ticker=ticker, sector=sector)
    if not ohlcv or len(ohlcv.get("closes", [])) < 30:
        ts.error = True
        return ts

    c = ohlcv["closes"]; h = ohlcv["highs"]
    l = ohlcv["lows"];   v = ohlcv["volumes"]
    raw = 0; lays = {}

    # ── CMF (50pt) ───────────────────────────────────────────────────────
    # 단일 최강 예측 지표 (ρ=+0.259)
    # 음→양 전환 타이밍이 핵심: 전환 중 > 완료 > 유지
    cmf_info = _cmf_turning(h, l, c, v)
    ts.cmf   = cmf_info["now"]
    a = 0
    if cmf_info["turning"] and cmf_info["positive"]:
        a += 50   # 음→양 전환 완료: 최강 매집 신호
    elif cmf_info["turning"]:
        a += 37   # 음수지만 방향 전환 중: 선제 진입 타이밍
    elif cmf_info["positive"]:
        a += 25   # 양수 유지: 매수 압력 지속
    elif cmf_info["now"] > -0.05:
        a += 10   # 중립: 관망
    else:
        a += 0    # 강한 매도 압력: 진입 불가

    # EvsR은 참고용으로 계산만 (점수 미반영, Slack 표시용)
    ts.evsr  = _evsr_absorption(h, l, c, v)
    ts.upvol = _upvol_ratio(c, v, period=10)
    ts.rsi   = _rsi(c)
    lays["A"] = a; raw += a

    # ── EvsR (30pt) ──────────────────────────────────────────────────────
    # 스마트머니 흡수 감지 (ρ=+0.102)
    # 고거래량인데 가격이 안 움직임 = 대량 매도 물량을 누군가 받아내는 중
    b = 0
    b += (30 if ts.evsr >= 2.5 else
          22  if ts.evsr >= 2.0 else
          15  if ts.evsr >= 1.5 else
           8  if ts.evsr >= 1.0 else 0)
    lays["B"] = b; raw += b

    # C, D: 보존 (재검증 예정)
    lays["C"] = 0
    lays["D"] = 0

    # ── 정규화 80→100 ────────────────────────
    score = round(raw / 80 * 100)
    score = min(score, 100)
    if   score >= 80: grade, ge = "Strong Buy", "🟢🟢"
    elif score >= 60: grade, ge = "Buy",        "🟢"
    elif score >= 40: grade, ge = "Neutral",    "⚪"
    elif score >= 20: grade, ge = "Caution",    "🟡"
    else:             grade, ge = "Avoid",      "🔴"

    ts.score = score; ts.raw = raw; ts.grade = grade
    ts.grade_emoji = ge; ts.layers = lays
    return ts



# ─────────────────────────────────────────────
# 섹터 집계
# ─────────────────────────────────────────────
def aggregate_sectors(results: list) -> dict:
    sectors = {}
    for ts in results:
        if ts.error:
            continue
        if ts.sector not in sectors:
            sectors[ts.sector] = {"scores": [], "tickers": []}
        sectors[ts.sector]["scores"].append(ts.score)
        sectors[ts.sector]["tickers"].append(ts.ticker)

    out = {}
    for s, data in sectors.items():
        avg = round(sum(data["scores"]) / len(data["scores"]))
        emoji, grade = (("🟢🟢","Strong") if avg >= 70 else
                        ("🟢","Bullish")  if avg >= 55 else
                        ("⚪","Neutral")  if avg >= 40 else
                        ("🟡","Bearish")  if avg >= 25 else
                        ("🔴","Weak"))
        out[s] = {"avg": avg, "count": len(data["scores"]),
                  "grade": grade, "emoji": emoji,
                  "tickers": data["tickers"]}
    return dict(sorted(out.items(), key=lambda x: -x[1]["avg"]))


# ─────────────────────────────────────────────
# Claude 섹터 코멘트
# ─────────────────────────────────────────────
def _claude_comment(sectors: dict, top15: list, bottom10: list) -> str:
    if not ANTHROPIC_API_KEY:
        return ""
    sec_lines = "\n".join(
        f"- {s}: {d['avg']}점 ({d['grade']}, {d['count']}종목)"
        for s, d in sectors.items()
    )
    top_s = ", ".join(f"{t.ticker}({t.score})" for t in top15[:8])
    bot_s = ", ".join(f"{t.ticker}({t.score})" for t in bottom10[:5])
    today = datetime.now(KST).strftime("%Y-%m-%d")

    prompt = f"""당신은 나스닥 100 섹터 분석 전문가입니다. {today} 기준 기술적 지표 스코어를 바탕으로 시장 현황을 한국어로 간결하게 해석해주세요.

섹터별 평균 기술점수 (100점 만점):
{sec_lines}

상위 종목: {top_s}
하위 종목: {bot_s}

다음 JSON으로만 응답 (markdown 없이):
{{"market_mood": "한 줄 시장 전체 분위기",
  "sector_comments": [{{"sector": "섹터명", "comment": "15자 이내 핵심 한줄"}}],
  "opportunity": "주목할 종목 또는 섹터 한줄 인사이트"}}"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "content-type": "application/json",
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 600,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"Claude API {r.status_code}")
            return ""
        text = "".join(b["text"] for b in r.json().get("content", []) if b.get("type") == "text")
        text = text.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        lines = [f"🌐 *{data.get('market_mood', '')}*"]
        for sc in data.get("sector_comments", []):
            lines.append(f"• {sc['sector']}: {sc['comment']}")
        if opp := data.get("opportunity", ""):
            lines.append(f"\n💡 {opp}")
        return "\n".join(lines)
    except Exception as e:
        log.warning(f"Claude error: {e}")
        return ""


# ─────────────────────────────────────────────
# Slack
# ─────────────────────────────────────────────
def _sec_block(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

def _div() -> dict:
    return {"type": "divider"}

def _ctx(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def build_blocks(results, sectors, top15, bottom10, claude_comment, elapsed):
    today    = datetime.now(KST).strftime("%m/%d")
    ok_count = sum(1 for r in results if not r.error)
    blocks   = []

    blocks.append({"type": "header", "text": {"type": "plain_text",
        "text": f"📡 S&P 500 기술지표 스캔 — {today}"}})
    blocks.append(_ctx(
        f"스캔 종목: *{ok_count}/{len(results)}* | "
        f"소요: {elapsed:.0f}초 | 일봉 6개월 · 4-Layer v2 (CMF·EvsR·ADX·BBSq)"
    ))
    blocks.append(_div())

    if claude_comment:
        blocks.append(_sec_block(claude_comment))
        blocks.append(_div())

    # 섹터 히트맵
    lines = ["*📊 섹터별 평균 점수*"]
    for s, d in sectors.items():
        fill = int(d["avg"] / 100 * 8)
        bar  = "█" * fill + "░" * (8 - fill)
        lines.append(f"{d['emoji']} `{bar}` *{d['avg']:2d}*  {s} ({d['count']}종목)")
    blocks.append(_sec_block("\n".join(lines)))
    blocks.append(_div())

    # Top 15
    lines = ["*🏆 Top 15 — 매수 신호 강도 순*"]
    for i, ts in enumerate(top15, 1):
        fill = int(ts.score / 100 * 6)
        bar  = "█" * fill + "░" * (6 - fill)
        squeeze_tag = " 🔥SQ" if ts.squeeze else ""
        lines.append(
            f"{i:2d}. {ts.grade_emoji} *${ts.ticker}* `{bar}` {ts.score}점"
            f"  RSI {ts.rsi:.0f}  MFI {ts.mfi:.0f}"
            f"  CMF {ts.cmf:+.2f}  ADX {ts.adx:.0f}{squeeze_tag}"
            f"  _{ts.sector}_"
        )
    blocks.append(_sec_block("\n".join(lines)))
    blocks.append(_div())

    # Bottom 10
    lines = ["*⚠️ Bottom 10 — 약세 경고*"]
    for i, ts in enumerate(bottom10, 1):
        lines.append(
            f"{i:2d}. {ts.grade_emoji} *${ts.ticker}*  {ts.score}점"
            f"  RSI {ts.rsi:.0f}  _{ts.sector}_"
        )
    blocks.append(_sec_block("\n".join(lines)))
    blocks.append(_div())

    # 레이어별 섹터 챔피언
    layer_names = {"A": "Volume/Flow", "B": "Trend", "C": "Momentum", "D": "Volatility"}
    lines = ["*🏅 레이어별 최강 섹터*"]
    for lid, lname in layer_names.items():
        best = max(
            ((s, round(
                sum(ts.layers.get(lid, 0) for ts in results
                    if not ts.error and ts.sector == s)
                / max(1, sum(1 for ts in results if not ts.error and ts.sector == s))
            )) for s in sectors),
            key=lambda x: x[1], default=("N/A", 0),
        )
        lines.append(f"• {lname}: *{best[0]}* ({best[1]:.0f}점)")
    blocks.append(_ctx("\n".join(lines)))
    blocks.append(_ctx("기술지표 기반 참고용. 투자 결정은 본인 판단하에. 다음 스캔: 내일 KST 07:00"))
    return blocks


def send_slack(blocks: list, text: str = "S&P 500 Market Scan"):
    if not SLACK_WEBHOOK:
        log.error("SLACK_WEBHOOK URL 없음")
        return
    for i in range(0, len(blocks), 40):
        chunk = blocks[i: i+40]
        try:
            r = requests.post(SLACK_WEBHOOK,
                              json={"blocks": chunk, "text": text},
                              timeout=15)
            log.info(f"Slack: {r.status_code} (블록 {i}~{i+len(chunk)-1})")
        except Exception as e:
            log.error(f"Slack 실패: {e}")


# ─────────────────────────────────────────────
# 단일 종목 Slack 블록 (--ticker 모드용)
# ─────────────────────────────────────────────
def _layer_bar(pts: int, max_pts: int, width: int = 8) -> str:
    fill = round(pts / max_pts * width) if max_pts else 0
    return "█" * fill + "░" * (width - fill)

def _pct(val: float) -> str:
    return f"{val:+.0%}" if abs(val) < 10 else f"{val:+.1f}"

def build_single_blocks(ts: TickerScore, elapsed: float) -> list:
    today  = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    sector = ts.sector or "Unknown"
    blocks = []

    # ── 헤더 ──────────────────────────────────
    blocks.append({"type": "header", "text": {"type": "plain_text",
        "text": f"🔍 ${ts.ticker} 단일 종목 스캔"}})
    blocks.append(_ctx(f"{today} | {elapsed:.1f}초 | 4-Layer v2"))
    blocks.append(_div())

    # ── 종합 점수 ─────────────────────────────
    fill = int(ts.score / 100 * 10)
    bar  = "█" * fill + "░" * (10 - fill)
    squeeze_tag = "  🔥 *BB Squeeze 발동*" if ts.squeeze else ""
    blocks.append(_sec_block(
        f"{ts.grade_emoji} *{ts.score}점*  `{bar}`  _{ts.grade}_{squeeze_tag}\n"
        f"섹터: *{sector}*"
    ))
    blocks.append(_div())

    # ── 레이어별 상세 ─────────────────────────
    max_pts = {"A": 28, "B": 20, "C": 20, "D": 12}
    layer_labels = {
        "A": "Volume / Flow",
        "B": "Trend",
        "C": "Momentum",
        "D": "Volatility · Entry",
    }
    detail = {
        "A": (f"CMF {ts.cmf:+.3f}  |  MFI {ts.mfi:.0f}"
              f"  |  EvsR {ts.evsr:.2f}{'  ⚡흡수' if ts.evsr >= 1.5 else ''}"),
        "B": (f"ADX {ts.adx:.0f}"
              f"{'(추세확립)' if ts.adx >= 25 else '(횡보)' if ts.adx < 15 else ''}"
              f"  |  MACD/EMA 기반"),
        "C": f"RSI {ts.rsi:.1f}  |  Stoch/Div 포함",
        "D": f"BB{'🔥Squeeze' if ts.squeeze else ''}  |  ATR 낙폭 기반",
    }
    lines = ["*📊 레이어별 상세*"]
    for lid, lname in layer_labels.items():
        pts  = ts.layers.get(lid, 0)
        mxp  = max_pts[lid]
        bar2 = _layer_bar(pts, mxp)
        lines.append(
            f"▸ *{lname}*  `{bar2}` {pts}/{mxp}pt\n"
            f"   _{detail[lid]}_"
        )
    blocks.append(_sec_block("\n".join(lines)))
    blocks.append(_div())

    # ── Claude 단일 코멘트 ────────────────────
    if ANTHROPIC_API_KEY:
        comment = _claude_single_comment(ts)
        if comment:
            blocks.append(_sec_block(comment))
            blocks.append(_div())

    blocks.append(_ctx(
        "기술지표 기반 참고용. 투자 결정은 본인 판단하에.\n"
        f"전체 스캔: `python market_scan.py` | 종목 스캔: `--ticker {ts.ticker}`"
    ))
    return blocks


def _claude_single_comment(ts: TickerScore) -> str:
    """단일 종목용 Claude 간단 코멘트"""
    try:
        prompt = (
            f"다음은 ${ts.ticker}({ts.sector}) 기술지표 스코어입니다 (100점 만점):\n"
            f"총점: {ts.score}점 ({ts.grade})\n"
            f"Layer A(Volume/Flow): {ts.layers.get('A',0)}/28 — CMF {ts.cmf:+.3f}, MFI {ts.mfi:.0f}, EvsR {ts.evsr:.2f}\n"
            f"Layer B(Trend): {ts.layers.get('B',0)}/20 — ADX {ts.adx:.0f}\n"
            f"Layer C(Momentum): {ts.layers.get('C',0)}/20 — RSI {ts.rsi:.1f}\n"
            f"Layer D(Volatility): {ts.layers.get('D',0)}/12 — BBSqueeze {'발동' if ts.squeeze else '없음'}\n\n"
            "이 데이터를 바탕으로 한국어로 2~3문장, 핵심만 날카롭게 평가해주세요. "
            "가격 예측 금지. 기술적 상태 진단만."
        )
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "content-type": "application/json",
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        if r.status_code != 200:
            return ""
        text = "".join(b["text"] for b in r.json().get("content", []) if b.get("type") == "text")
        return f"🤖 *Claude 진단*\n{text.strip()}"
    except Exception:
        return ""


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    # ── CLI 파싱 ──────────────────────────────
    parser = argparse.ArgumentParser(description="S&P 500 Market Scanner v3.0")
    parser.add_argument("--ticker", type=str, default="",
                        help="단일 종목 스캔 (예: --ticker HOOD). 없으면 전체 스캔.")
    args = parser.parse_args()

    single_ticker = args.ticker.strip().upper()

    # ══════════════════════════════════════════
    # 모드 A: 단일 종목 스캔
    # ══════════════════════════════════════════
    if single_ticker:
        log.info(f"=== 단일 종목 스캔: ${single_ticker} ===")
        start = time.time()

        sector = SP500.get(single_ticker, "Unknown")
        ohlcv_map = batch_download([single_ticker], period="6mo")
        ts = score_ticker(single_ticker, sector, ohlcv_map.get(single_ticker, {}))

        elapsed = time.time() - start
        if ts.error:
            log.error(f"${single_ticker}: 데이터 없음 또는 다운로드 실패")
            sys.exit(1)

        log.info(
            f"${single_ticker}: {ts.score}점 ({ts.grade}) | "
            f"RSI={ts.rsi:.1f} CMF={ts.cmf:+.3f} ADX={ts.adx:.0f} "
            f"EvsR={ts.evsr:.2f} Squeeze={ts.squeeze} | {elapsed:.1f}초"
        )
        blocks = build_single_blocks(ts, elapsed)
        send_slack(blocks, text=f"${single_ticker} 단일 종목 스캔")
        log.info("=== 완료 ===")
        return

    # ══════════════════════════════════════════
    # 모드 B: S&P 500 전체 스캔
    # ══════════════════════════════════════════
    log.info(f"=== S&P 500 Market Scan v3.0 시작: {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')} ===")
    start = time.time()

    tickers = list(SP500.keys())
    log.info(f"대상: {len(tickers)}종목 — yfinance 배치 다운로드 (청크 25, 딜레이 3s)")

    # 전종목 배치 다운로드
    ohlcv_map = batch_download(tickers, period="6mo")
    log.info(f"다운로드 완료: {len(ohlcv_map)}/{len(tickers)}종목")

    # 스코어링 (로컬 계산)
    results = []
    for ticker, sector in SP500.items():
        ts = score_ticker(ticker, sector, ohlcv_map.get(ticker, {}))
        if not ts.error:
            log.info(f"  {ticker}: {ts.score}점 ({ts.grade}) RSI={ts.rsi:.1f}")
        else:
            log.warning(f"  {ticker}: 데이터 없음")
        results.append(ts)

    ok_results = [r for r in results if not r.error]
    sectors    = aggregate_sectors(ok_results)
    top15      = sorted(ok_results, key=lambda x: -x.score)[:15]
    bottom10   = sorted(ok_results, key=lambda x:  x.score)[:10]

    elapsed = time.time() - start
    log.info(f"스캔 완료: {len(ok_results)}/{len(results)}종목, {elapsed:.1f}초")

    claude_comment = _claude_comment(sectors, top15, bottom10)
    blocks = build_blocks(results, sectors, top15, bottom10, claude_comment, elapsed)
    send_slack(blocks, text="S&P 500 Market Scan")
    log.info("=== 완료 ===")


if __name__ == "__main__":
    main()
