#!/usr/bin/env python3
"""
Nasdaq 100 Market Scanner v1.0
================================
매일 KST 07:00 실행 (장 마감 후 확정 데이터 기준)
- 전종목 5-Layer 기술지표 스코어링 (100점)
- 섹터별 평균 + 강약 분석
- Top 15 / Bottom 10 추출
- Claude 섹터 코멘트
"""

import os
import sys
import time
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
SLACK_WEBHOOK = os.environ.get("MARKET_SCAN_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("market_scan")

# ─────────────────────────────────────────────
# Nasdaq 100 종목 + 섹터 매핑
# ─────────────────────────────────────────────
NDX100 = {
    # Technology
    "AAPL":  "Technology", "MSFT":  "Technology", "NVDA":  "Technology",
    "AVGO":  "Technology", "ORCL":  "Technology", "CSCO":  "Technology",
    "ADBE":  "Technology", "TXN":   "Technology", "QCOM":  "Technology",
    "AMD":   "Technology", "AMAT":  "Technology", "MU":    "Technology",
    "INTC":  "Technology", "LRCX":  "Technology", "KLAC":  "Technology",
    "MRVL":  "Technology", "CDNS":  "Technology", "SNPS":  "Technology",
    "FTNT":  "Technology", "ANSS":  "Technology", "ON":    "Technology",
    "NXPI":  "Technology", "MCHP":  "Technology",
    # Communication Services
    "META":  "Comm Services", "GOOGL": "Comm Services", "GOOG":  "Comm Services",
    "NFLX":  "Comm Services", "TMUS":  "Comm Services",
    # Consumer Discretionary
    "AMZN":  "Cons Discretionary", "TSLA":  "Cons Discretionary",
    "BKNG":  "Cons Discretionary", "MCD":   "Cons Discretionary",
    "SBUX":  "Cons Discretionary", "CMG":   "Cons Discretionary",
    "ABNB":  "Cons Discretionary", "MAR":   "Cons Discretionary",
    "ORLY":  "Cons Discretionary", "AZO":   "Cons Discretionary",
    "ROST":  "Cons Discretionary",
    # Healthcare
    "AMGN":  "Healthcare", "GILD":  "Healthcare", "VRTX":  "Healthcare",
    "REGN":  "Healthcare", "MRNA":  "Healthcare", "BIIB":  "Healthcare",
    "IDXX":  "Healthcare", "DXCM":  "Healthcare", "ALGN":  "Healthcare",
    "ILMN":  "Healthcare",
    # Financials
    "PYPL":  "Financials", "INTC":  "Technology",  # INTC already above
    # Consumer Staples
    "PEP":   "Cons Staples", "COST":  "Cons Staples", "MDLZ":  "Cons Staples",
    "KHC":   "Cons Staples", "MNST":  "Cons Staples",
    # Industrials
    "HON":   "Industrials", "CTAS":  "Industrials", "PAYX":  "Industrials",
    "FAST":  "Industrials", "ODFL":  "Industrials",
    # Software / Cloud (부분적으로 Tech 중복이지만 세분화)
    "CRM":   "Technology", "NOW":   "Technology", "PANW":  "Technology",
    "CRWD":  "Technology", "TEAM":  "Technology", "ZS":    "Technology",
    "DDOG":  "Technology", "SPLK":  "Technology", "WDAY":  "Technology",
    "OKTA":  "Technology", "SNOW":  "Technology", "NET":   "Technology",
    "HUBS":  "Technology", "MDB":   "Technology",
    # Semiconductors (already in Tech but notable)
    "ASML":  "Technology", "TSM":   "Technology",
    # Other
    "ISRG":  "Healthcare", "INTU":  "Technology", "ADP":   "Technology",
    "VRSK":  "Industrials", "CPRT":  "Industrials", "FANG":  "Energy",
    "EXC":   "Utilities", "XEL":   "Utilities",
    "GEHC":  "Healthcare", "CEG":   "Utilities",
    # Fintech / Financial
    "COIN":  "Financials",
}

# 중복 제거 및 최종 리스트
NDX100 = {k: v for k, v in NDX100.items()}


# ─────────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────────
@dataclass
class TickerScore:
    ticker: str = ""
    sector: str = ""
    score: int = 0          # 0~100 (정규화)
    raw: int = 0            # 0~80 (원점수)
    grade: str = ""
    grade_emoji: str = ""
    rsi: float = 50.0
    mfi: float = 50.0
    layers: dict = field(default_factory=dict)  # layer_id → pts
    error: bool = False


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
_last_request_time = 0.0

def _throttle(delay: float = 0.4):
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_request_time = time.time()


def _safe_get(url, params=None, timeout=12):
    try:
        _throttle()
        r = requests.get(url, params=params,
                         headers={"User-Agent": BROWSER_UA}, timeout=timeout)
        if r.status_code == 200:
            return r
        log.warning(f"HTTP {r.status_code}: {url[:60]}")
    except Exception as e:
        log.warning(f"Request error: {e}")
    return None


# ─────────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────────
def _calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    diffs = [closes[i] - closes[i - 1] for i in range(len(closes) - period, len(closes))]
    gains = sum(d for d in diffs if d > 0) / period
    losses = sum(-d for d in diffs if d < 0) / period
    if losses == 0:
        return 100.0
    return round(100 - 100 / (1 + gains / losses), 2)


def _calc_macd_hist(closes: list) -> float:
    def ema(data, p):
        if len(data) < p:
            return [0.0] * len(data)
        k = 2 / (p + 1)
        r = [sum(data[:p]) / p]
        for v in data[p:]:
            r.append(v * k + r[-1] * (1 - k))
        return r
    if len(closes) < 35:
        return 0.0
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    n = min(len(e12), len(e26))
    macd = [e12[i] - e26[i] for i in range(n)]
    sig = ema(macd, 9)
    return round(macd[-1] - sig[-1], 6)


def _calc_ema(data: list, period: int) -> list:
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    r = [sum(data[:period]) / period]
    for v in data[period:]:
        r.append(v * k + r[-1] * (1 - k))
    return r


def _calc_obv(closes: list, volumes: list) -> list:
    obv = [0]
    for i in range(1, min(len(closes), len(volumes))):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    return obv


def _calc_mfi(highs, lows, closes, volumes, period=14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 1:
        return None
    tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(n)]
    pos = neg = 0.0
    for i in range(n - period, n):
        mf = tp[i] * volumes[i]
        if tp[i] > tp[i - 1]:
            pos += mf
        else:
            neg += mf
    return round(100 - 100 / (1 + pos / neg), 2) if neg else 100.0


def _calc_stoch(highs, lows, closes, period=14, sk=3, sd=3) -> tuple:
    n = min(len(highs), len(lows), len(closes))
    if n < period + sk + sd:
        return None, None
    raw_k = []
    for i in range(period - 1, n):
        hh = max(highs[i - period + 1: i + 1])
        ll = min(lows[i - period + 1: i + 1])
        raw_k.append((closes[i] - ll) / (hh - ll) * 100 if hh != ll else 50.0)
    ks = [sum(raw_k[i - sk + 1: i + 1]) / sk for i in range(sk - 1, len(raw_k))]
    if len(ks) < sd:
        return None, None
    return round(ks[-1], 2), round(sum(ks[-sd:]) / sd, 2)


def _calc_atr(highs, lows, closes, period=14) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i - 1]),
               abs(lows[i] - closes[i - 1])) for i in range(1, n)]
    return sum(trs[-period:]) / period if len(trs) >= period else None


def _rsi_divergence(closes: list, lookback: int = 20) -> bool:
    if len(closes) < lookback + 14:
        return False
    window = closes[-(lookback + 14):]
    rsi_s = [_calc_rsi(window[:i + 1]) for i in range(14, len(window))]
    if len(rsi_s) < lookback:
        return False
    pw = closes[-lookback:]
    rw = rsi_s[-lookback:]
    mid = len(pw) // 2
    return min(pw[mid:]) < min(pw[:mid]) and min(rw[mid:]) > min(rw[:mid])


# ─────────────────────────────────────────────
# OHLCV fetch
# ─────────────────────────────────────────────
def _fetch_ohlcv(ticker: str, days: int = 90) -> dict:
    """일봉 OHLCV — 90일이면 RSI/MACD/BB/Stoch 모두 계산 가능"""
    url = YAHOO_QUOTE_URL.format(ticker=ticker)
    resp = _safe_get(url, params={"interval": "1d", "range": f"{days}d"})
    if not resp:
        return {}
    try:
        q = resp.json()["chart"]["result"][0]["indicators"]["quote"][0]
        def _c(lst): return [v if v else 0.0 for v in lst]
        return {
            "closes":  _c(q.get("close", [])),
            "highs":   _c(q.get("high", [])),
            "lows":    _c(q.get("low", [])),
            "volumes": [int(v) if v else 0 for v in q.get("volume", [])],
        }
    except Exception as e:
        log.debug(f"OHLCV {ticker}: {e}")
        return {}


# ─────────────────────────────────────────────
# 5-Layer 스코어 (80점 만점 → 100점 정규화)
# 주봉/Whale/POC 제외 (API 호출 최소화)
# ─────────────────────────────────────────────
def score_ticker(ticker: str, sector: str) -> TickerScore:
    ts = TickerScore(ticker=ticker, sector=sector)
    ohlcv = _fetch_ohlcv(ticker)
    if not ohlcv or len(ohlcv.get("closes", [])) < 30:
        ts.error = True
        return ts

    c = ohlcv["closes"]
    h = ohlcv["highs"]
    l = ohlcv["lows"]
    v = ohlcv["volumes"]

    raw = 0
    layers = {}

    # ── A. Volume / Flow (28pts) ────────────
    a = 0
    # OBV divergence (10pts)
    obv = _calc_obv(c, v)
    if len(obv) >= 6:
        p5 = c[-1] - c[-6]
        o5 = obv[-1] - obv[-6]
        if p5 < 0 and o5 > 0:   a += 10
        elif p5 < 0 and o5 < 0: a += 0
        elif p5 > 0 and o5 > 0: a += 5
        else:                    a += 2
    # MFI (10pts)
    mfi = _calc_mfi(h, l, c, v)
    ts.mfi = mfi if mfi is not None else 50.0
    if mfi is not None:
        if   mfi < 20: a += 10
        elif mfi < 30: a += 7
        elif mfi < 40: a += 4
        elif mfi > 80: a += 0
        else:          a += 1
    # Volume contraction (8pts)
    if len(v) >= 20:
        v3  = sum(x for x in v[-3:]  if x > 0) / 3
        v20 = sum(x for x in v[-20:] if x > 0) / 20
        ratio = v3 / v20 if v20 > 0 else 1.0
        if   ratio < 0.70: a += 8
        elif ratio < 0.85: a += 5
        elif ratio < 1.00: a += 2
    layers["A"] = a
    raw += a

    # ── B. Trend (20pts) ───────────────────
    b = 0
    # MACD histogram 수렴 (10pts)
    mh = _calc_macd_hist(c)
    if len(c) >= 35:
        prev_mh = _calc_macd_hist(c[:-1])
        converging = mh < 0 and mh > prev_mh
        if converging:  b += 10
        elif mh > 0 and mh > prev_mh: b += 7
        elif mh > 0:    b += 5
        else:           b += 2
    # EMA 구조 (10pts)
    e20  = _calc_ema(c, 20)
    e50  = _calc_ema(c, 50)
    cur  = c[-1]
    if e20 and e50:
        v20, v50 = e20[-1], e50[-1]
        if cur < v20 and v20 > v50:  b += 10
        elif cur < v50:              b += 7
        elif cur > v20 > v50:        b += 5
        elif v20 < v50:              b += 2
        else:                        b += 4
    layers["B"] = b
    raw += b

    # ── C. Momentum (20pts) ────────────────
    c_pts = 0
    # RSI (8pts)
    rsi = _calc_rsi(c)
    ts.rsi = rsi
    if   rsi <= 25: c_pts += 8
    elif rsi <= 30: c_pts += 7
    elif rsi <= 40: c_pts += 5
    elif rsi <= 50: c_pts += 2
    elif rsi >= 70: c_pts += 0
    else:           c_pts += 1
    # Stochastic (7pts)
    sk_v, sd_v = _calc_stoch(h, l, c)
    if sk_v is not None and sd_v is not None:
        if   sk_v < 20 and sd_v < 20 and sk_v > sd_v: c_pts += 7
        elif sk_v < 20 and sd_v < 20:                  c_pts += 4
        elif sk_v < 50 and sk_v > sd_v:                c_pts += 2
        elif sk_v > 80:                                 c_pts += 0
        else:                                           c_pts += 1
    # RSI divergence (5pts)
    if _rsi_divergence(c):
        c_pts += 5
    layers["C"] = c_pts
    raw += c_pts

    # ── D. Volatility / Entry (12pts) ──────
    d = 0
    # BB 위치 (7pts)
    if len(c) >= 20:
        sma = sum(c[-20:]) / 20
        std = (sum((x - sma) ** 2 for x in c[-20:]) / 20) ** 0.5
        lower = sma - 2 * std
        upper = sma + 2 * std
        if cur < lower:                         d += 7
        elif cur < lower * 1.03:                d += 5
        elif cur > upper:                       d += 0
        else:                                   d += 2
    # ATR 맥락 (5pts)
    atr = _calc_atr(h, l, c)
    if atr and atr > 0 and len(c) >= 20:
        hi20 = max(c[-20:])
        dd   = abs(cur - hi20)
        mult = dd / atr
        if   mult < 1.5: d += 5
        elif mult < 2.5: d += 2
        else:            d += 0
    layers["D"] = d
    raw += d

    # ── 정규화 (80점 → 100점) ──────────────
    score = round(raw / 80 * 100)

    if   score >= 80: grade, grade_emoji = "Strong Buy", "🟢🟢"
    elif score >= 60: grade, grade_emoji = "Buy",         "🟢"
    elif score >= 40: grade, grade_emoji = "Neutral",     "⚪"
    elif score >= 20: grade, grade_emoji = "Caution",     "🟡"
    else:             grade, grade_emoji = "Avoid",       "🔴"

    ts.score = score
    ts.raw   = raw
    ts.grade = grade
    ts.grade_emoji = grade_emoji
    ts.layers = layers
    return ts


# ─────────────────────────────────────────────
# 섹터 집계
# ─────────────────────────────────────────────
def aggregate_sectors(results: list) -> dict:
    """섹터별 평균 점수, 종목 수, 강약 등급"""
    sectors = {}
    for ts in results:
        if ts.error:
            continue
        s = ts.sector
        if s not in sectors:
            sectors[s] = {"scores": [], "tickers": []}
        sectors[s]["scores"].append(ts.score)
        sectors[s]["tickers"].append(ts.ticker)

    out = {}
    for s, data in sectors.items():
        avg = round(sum(data["scores"]) / len(data["scores"]))
        if   avg >= 70: grade, emoji = "Strong",  "🟢🟢"
        elif avg >= 55: grade, emoji = "Bullish",  "🟢"
        elif avg >= 40: grade, emoji = "Neutral",  "⚪"
        elif avg >= 25: grade, emoji = "Bearish",  "🟡"
        else:           grade, emoji = "Weak",     "🔴"
        out[s] = {
            "avg": avg, "count": len(data["scores"]),
            "grade": grade, "emoji": emoji,
            "tickers": data["tickers"],
        }
    return dict(sorted(out.items(), key=lambda x: -x[1]["avg"]))


# ─────────────────────────────────────────────
# Claude 섹터 코멘트
# ─────────────────────────────────────────────
def _claude_sector_comment(sectors: dict, top15: list, bottom10: list) -> str:
    if not ANTHROPIC_API_KEY:
        return ""
    sector_lines = "\n".join(
        f"- {s}: {d['avg']}점 ({d['grade']}, {d['count']}종목)"
        for s, d in sectors.items()
    )
    top_lines = ", ".join(f"{t.ticker}({t.score})" for t in top15[:8])
    bot_lines = ", ".join(f"{t.ticker}({t.score})" for t in bottom10[:5])
    today = datetime.now(KST).strftime("%Y-%m-%d")

    prompt = f"""당신은 나스닥 100 섹터 분석 전문가입니다. {today} 기준 기술적 지표 스코어를 바탕으로 시장 현황을 한국어로 간결하게 해석해주세요.

섹터별 평균 기술점수 (100점 만점):
{sector_lines}

상위 종목: {top_lines}
하위 종목: {bot_lines}

다음 형식의 JSON으로만 응답하세요:
{{"market_mood": "한 줄 시장 전체 분위기 (예: '기술주 전반 과매도, 반등 시도 구간')",
  "sector_comments": [
    {{"sector": "섹터명", "comment": "15자 이내 핵심 한줄"}},
    ...
  ],
  "opportunity": "주목할 종목 또는 섹터 한줄 인사이트"
}}"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 600,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"Claude API {r.status_code}")
            return ""
        text = ""
        for block in r.json().get("content", []):
            if block.get("type") == "text":
                text += block["text"]
        text = text.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        lines = [f"🌐 *{data.get('market_mood', '')}*"]
        for sc in data.get("sector_comments", []):
            lines.append(f"• {sc['sector']}: {sc['comment']}")
        opp = data.get("opportunity", "")
        if opp:
            lines.append(f"\n💡 {opp}")
        return "\n".join(lines)
    except Exception as e:
        log.warning(f"Claude comment error: {e}")
        return ""


# ─────────────────────────────────────────────
# Slack 포맷
# ─────────────────────────────────────────────
def _sec(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

def _div() -> dict:
    return {"type": "divider"}

def _ctx(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def build_slack_blocks(
    results: list,
    sectors: dict,
    top15: list,
    bottom10: list,
    claude_comment: str,
    elapsed_sec: float,
) -> list:
    today = datetime.now(KST).strftime("%m/%d")
    ok_count = sum(1 for r in results if not r.error)
    blocks = []

    # ── 헤더 ──────────────────────────────────────────
    blocks.append({"type": "header", "text": {"type": "plain_text",
        "text": f"📡 NDX 100 기술지표 스캔 — {today}"}})

    blocks.append(_ctx(
        f"스캔 종목: *{ok_count}/{len(results)}* | "
        f"소요: {elapsed_sec:.0f}초 | "
        f"기준: 일봉 90일 · 4-Layer 100점"
    ))
    blocks.append(_div())

    # ── Claude 코멘트 ──────────────────────────────────
    if claude_comment:
        blocks.append(_sec(claude_comment))
        blocks.append(_div())

    # ── 섹터 히트맵 ───────────────────────────────────
    sector_lines = ["*📊 섹터별 평균 점수*"]
    for s, d in sectors.items():
        bar_fill = int(d["avg"] / 100 * 8)
        bar = "█" * bar_fill + "░" * (8 - bar_fill)
        sector_lines.append(
            f"{d['emoji']} `{bar}` *{d['avg']:2d}* {s} ({d['count']}종목)"
        )
    blocks.append(_sec("\n".join(sector_lines)))
    blocks.append(_div())

    # ── Top 15 ─────────────────────────────────────────
    top_lines = ["*🏆 Top 15 — 매수 신호 강도 순*"]
    for i, ts in enumerate(top15, 1):
        bar_fill = int(ts.score / 100 * 6)
        bar = "█" * bar_fill + "░" * (6 - bar_fill)
        top_lines.append(
            f"{i:2d}. {ts.grade_emoji} *${ts.ticker}* `{bar}` {ts.score}점"
            f"  RSI {ts.rsi:.0f}  MFI {ts.mfi:.0f}  _{ts.sector}_"
        )
    blocks.append(_sec("\n".join(top_lines)))
    blocks.append(_div())

    # ── Bottom 10 ──────────────────────────────────────
    bot_lines = ["*⚠️ Bottom 10 — 약세 경고*"]
    for i, ts in enumerate(bottom10, 1):
        bot_lines.append(
            f"{i:2d}. {ts.grade_emoji} *${ts.ticker}*  {ts.score}점"
            f"  RSI {ts.rsi:.0f}  _{ts.sector}_"
        )
    blocks.append(_sec("\n".join(bot_lines)))
    blocks.append(_div())

    # ── 레이어별 섹터 챔피언 ───────────────────────────
    layer_names = {"A": "Volume/Flow", "B": "Trend", "C": "Momentum", "D": "Volatility"}
    champ_lines = ["*🏅 레이어별 최강 섹터*"]
    for lid, lname in layer_names.items():
        best_sector = max(
            ((s, round(
                sum(ts.layers.get(lid, 0) for ts in results
                    if not ts.error and ts.sector == s)
                / max(1, sum(1 for ts in results if not ts.error and ts.sector == s))
            , 1)) for s in sectors),
            key=lambda x: x[1],
            default=("N/A", 0),
        )
        champ_lines.append(f"• {lname}: *{best_sector[0]}* ({best_sector[1]:.0f}점)")
    blocks.append(_ctx("\n".join(champ_lines)))

    # ── footer ────────────────────────────────────────
    blocks.append(_ctx(
        f"기술지표 기반 참고용 스코어. 투자 결정은 본인 판단하에. "
        f"다음 스캔: 내일 KST 07:00"
    ))
    return blocks


# ─────────────────────────────────────────────
# Slack 전송
# ─────────────────────────────────────────────
def send_slack(blocks: list):
    if not SLACK_WEBHOOK:
        log.error("SLACK_WEBHOOK URL 없음")
        return
    # Slack 블록 3000자 제한 대비 분할
    chunk_size = 40
    for i in range(0, len(blocks), chunk_size):
        chunk = blocks[i: i + chunk_size]
        try:
            r = requests.post(SLACK_WEBHOOK,
                              json={"blocks": chunk, "text": "NDX 100 Market Scan"},
                              timeout=15)
            log.info(f"Slack 전송: {r.status_code} (블록 {i}~{i+len(chunk)-1})")
        except Exception as e:
            log.error(f"Slack 전송 실패: {e}")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    log.info(f"=== NDX 100 Market Scan 시작: {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')} ===")
    start = time.time()

    tickers = list(NDX100.items())
    log.info(f"스캔 대상: {len(tickers)}종목")

    results = []
    for i, (ticker, sector) in enumerate(tickers):
        log.info(f"[{i+1}/{len(tickers)}] {ticker} ({sector})")
        ts = score_ticker(ticker, sector)
        if ts.error:
            log.warning(f"  {ticker}: 데이터 없음")
        else:
            log.info(f"  {ticker}: {ts.score}점 ({ts.grade}) RSI={ts.rsi}")
        results.append(ts)

    # 집계
    ok_results = [r for r in results if not r.error]
    sectors    = aggregate_sectors(ok_results)
    top15      = sorted(ok_results, key=lambda x: -x.score)[:15]
    bottom10   = sorted(ok_results, key=lambda x: x.score)[:10]

    elapsed = time.time() - start
    log.info(f"스캔 완료: {len(ok_results)}/{len(results)}종목, {elapsed:.1f}초")

    # Claude 코멘트
    claude_comment = _claude_sector_comment(sectors, top15, bottom10)

    # Slack 전송
    blocks = build_slack_blocks(results, sectors, top15, bottom10, claude_comment, elapsed)
    send_slack(blocks)
    log.info("=== 완료 ===")


if __name__ == "__main__":
    main()
