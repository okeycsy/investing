#!/usr/bin/env python3
"""
Nasdaq 100 Market Scanner v2.0
================================
v2.0: yfinance 배치 다운로드 방식으로 전환
- 전종목을 요청 2~3번에 처리 (개별 87번 → 청크 3번)
- 429 문제 근본 해결
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
# Nasdaq 100 종목 + 섹터
# ─────────────────────────────────────────────
NDX100 = {
    # Technology
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "AVGO": "Technology", "ORCL": "Technology", "CSCO": "Technology",
    "ADBE": "Technology", "TXN":  "Technology", "QCOM": "Technology",
    "AMD":  "Technology", "AMAT": "Technology", "MU":   "Technology",
    "INTC": "Technology", "LRCX": "Technology", "KLAC": "Technology",
    "MRVL": "Technology", "CDNS": "Technology", "SNPS": "Technology",
    "FTNT": "Technology", "ANSS": "Technology", "ON":   "Technology",
    "NXPI": "Technology", "MCHP": "Technology", "ASML": "Technology",
    "TSM":  "Technology", "INTU": "Technology", "ADP":  "Technology",
    "CRM":  "Technology", "NOW":  "Technology", "PANW": "Technology",
    "CRWD": "Technology", "TEAM": "Technology", "ZS":   "Technology",
    "DDOG": "Technology", "WDAY": "Technology", "SNOW": "Technology",
    "NET":  "Technology", "HUBS": "Technology", "MDB":  "Technology",
    # Communication Services
    "META":  "Comm Services", "GOOGL": "Comm Services", "GOOG": "Comm Services",
    "NFLX":  "Comm Services", "TMUS":  "Comm Services",
    # Consumer Discretionary
    "AMZN": "Cons Discretionary", "TSLA": "Cons Discretionary",
    "BKNG": "Cons Discretionary", "MCD":  "Cons Discretionary",
    "SBUX": "Cons Discretionary", "CMG":  "Cons Discretionary",
    "ABNB": "Cons Discretionary", "MAR":  "Cons Discretionary",
    "ORLY": "Cons Discretionary", "AZO":  "Cons Discretionary",
    "ROST": "Cons Discretionary",
    # Healthcare
    "AMGN": "Healthcare", "GILD": "Healthcare", "VRTX": "Healthcare",
    "REGN": "Healthcare", "MRNA": "Healthcare", "BIIB": "Healthcare",
    "IDXX": "Healthcare", "DXCM": "Healthcare", "ISRG": "Healthcare",
    "GEHC": "Healthcare",
    # Consumer Staples
    "PEP":  "Cons Staples", "COST": "Cons Staples", "MDLZ": "Cons Staples",
    "KHC":  "Cons Staples", "MNST": "Cons Staples",
    # Industrials
    "HON":  "Industrials", "CTAS": "Industrials", "PAYX": "Industrials",
    "FAST": "Industrials", "ODFL": "Industrials", "VRSK": "Industrials",
    "CPRT": "Industrials",
    # Financials
    "PYPL": "Financials", "COIN": "Financials",
    # Energy
    "FANG": "Energy",
    # Utilities
    "EXC": "Utilities", "XEL": "Utilities", "CEG": "Utilities",
}


# ─────────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────────
@dataclass
class TickerScore:
    ticker: str = ""
    sector: str = ""
    score: int = 0
    raw: int = 0
    grade: str = ""
    grade_emoji: str = ""
    rsi: float = 50.0
    mfi: float = 50.0
    layers: dict = field(default_factory=dict)
    error: bool = False


# ─────────────────────────────────────────────
# 배치 OHLCV 다운로드 (핵심: 전종목 한번에)
# ─────────────────────────────────────────────
def batch_download(tickers: list, period: str = "6mo") -> dict:
    """
    yfinance 배치 다운로드 — 전종목을 청크(30종목)로 나눠 요청.
    반환: {ticker: {"closes":[], "highs":[], "lows":[], "volumes":[]}}
    """
    result = {}
    chunk_size = 30  # 한 번에 30종목씩 (안정성)

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
            time.sleep(2)

    return result


# ─────────────────────────────────────────────
# 지표 계산 (순수 파이썬, 의존성 없음)
# ─────────────────────────────────────────────
def _rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    diffs = [closes[i] - closes[i-1] for i in range(len(closes)-period, len(closes))]
    g = sum(d for d in diffs if d > 0) / period
    l = sum(-d for d in diffs if d < 0) / period
    return round(100 - 100 / (1 + g / l), 2) if l else 100.0


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


def _obv(closes: list, volumes: list) -> list:
    obv = [0]
    for i in range(1, min(len(closes), len(volumes))):
        obv.append(obv[-1] + volumes[i] if closes[i] > closes[i-1]
                   else obv[-1] - volumes[i] if closes[i] < closes[i-1]
                   else obv[-1])
    return obv


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


# ─────────────────────────────────────────────
# 5-Layer 스코어 (80점 만점 → 100점 정규화)
# ─────────────────────────────────────────────
def score_ticker(ticker: str, sector: str, ohlcv: dict) -> TickerScore:
    ts = TickerScore(ticker=ticker, sector=sector)
    if not ohlcv or len(ohlcv.get("closes", [])) < 30:
        ts.error = True
        return ts

    c = ohlcv["closes"]; h = ohlcv["highs"]
    l = ohlcv["lows"];   v = ohlcv["volumes"]
    raw = 0; lays = {}

    # ── A. Volume / Flow (28pts) ──────────────
    a = 0
    obv_s = _obv(c, v)
    if len(obv_s) >= 6:
        p5 = c[-1] - c[-6]; o5 = obv_s[-1] - obv_s[-6]
        a += 10 if p5 < 0 and o5 > 0 else 0 if p5 < 0 else 5 if p5 > 0 and o5 > 0 else 2

    mfi_v = _mfi(h, l, c, v); ts.mfi = mfi_v if mfi_v else 50.0
    if mfi_v is not None:
        a += 10 if mfi_v < 20 else 7 if mfi_v < 30 else 4 if mfi_v < 40 else 0 if mfi_v > 80 else 1

    if len(v) >= 20:
        v3  = sum(x for x in v[-3:]  if x > 0) / 3
        v20 = sum(x for x in v[-20:] if x > 0) / 20
        r   = v3 / v20 if v20 else 1.0
        a += 8 if r < 0.70 else 5 if r < 0.85 else 2 if r < 1.00 else 0

    lays["A"] = a; raw += a

    # ── B. Trend (20pts) ─────────────────────
    b = 0
    mh, prev_mh = _macd_hist(c)
    if len(c) >= 35:
        b += (10 if mh < 0 and mh > prev_mh else
              7  if mh > 0 and mh > prev_mh else
              5  if mh > 0 else 2)

    e20 = _ema(c, 20); e50 = _ema(c, 50); cur = c[-1]
    if e20 and e50:
        v20e, v50e = e20[-1], e50[-1]
        b += (10 if cur < v20e and v20e > v50e else
              7  if cur < v50e else
              5  if cur > v20e > v50e else
              2  if v20e < v50e else 4)

    lays["B"] = b; raw += b

    # ── C. Momentum (20pts) ──────────────────
    cp = 0
    rsi_v = _rsi(c); ts.rsi = rsi_v
    cp += (8 if rsi_v <= 25 else 7 if rsi_v <= 30 else
           5 if rsi_v <= 40 else 2 if rsi_v <= 50 else
           0 if rsi_v >= 70 else 1)

    sk_v, sd_v = _stoch(h, l, c)
    if sk_v is not None and sd_v is not None:
        cp += (7 if sk_v < 20 and sd_v < 20 and sk_v > sd_v else
               4 if sk_v < 20 and sd_v < 20 else
               2 if sk_v < 50 and sk_v > sd_v else
               0 if sk_v > 80 else 1)

    if _rsi_divergence(c):
        cp += 5

    lays["C"] = cp; raw += cp

    # ── D. Volatility / Entry (12pts) ────────
    d = 0
    if len(c) >= 20:
        sma   = sum(c[-20:]) / 20
        std   = (sum((x-sma)**2 for x in c[-20:]) / 20) ** 0.5
        lower = sma - 2 * std; upper = sma + 2 * std
        d += (7 if cur < lower else
              5 if cur < lower * 1.03 else
              0 if cur > upper else 2)

    atr_v = _atr(h, l, c)
    if atr_v and atr_v > 0 and len(c) >= 20:
        hi20 = max(c[-20:]); mult = abs(cur - hi20) / atr_v
        d += 5 if mult < 1.5 else 2 if mult < 2.5 else 0

    lays["D"] = d; raw += d

    # ── 정규화 80→100 ────────────────────────
    score = round(raw / 80 * 100)
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
        "text": f"📡 NDX 100 기술지표 스캔 — {today}"}})
    blocks.append(_ctx(
        f"스캔 종목: *{ok_count}/{len(results)}* | "
        f"소요: {elapsed:.0f}초 | 일봉 6개월 · 4-Layer 100점"
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
        lines.append(
            f"{i:2d}. {ts.grade_emoji} *${ts.ticker}* `{bar}` {ts.score}점"
            f"  RSI {ts.rsi:.0f}  MFI {ts.mfi:.0f}  _{ts.sector}_"
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


def send_slack(blocks: list):
    if not SLACK_WEBHOOK:
        log.error("SLACK_WEBHOOK URL 없음")
        return
    for i in range(0, len(blocks), 40):
        chunk = blocks[i: i+40]
        try:
            r = requests.post(SLACK_WEBHOOK,
                              json={"blocks": chunk, "text": "NDX 100 Market Scan"},
                              timeout=15)
            log.info(f"Slack: {r.status_code} (블록 {i}~{i+len(chunk)-1})")
        except Exception as e:
            log.error(f"Slack 실패: {e}")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    log.info(f"=== NDX 100 Market Scan v2.0 시작: {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')} ===")
    start = time.time()

    tickers = list(NDX100.keys())
    log.info(f"대상: {len(tickers)}종목 — yfinance 배치 다운로드")

    # 전종목 배치 다운로드 (핵심: 요청 3번으로 끝)
    ohlcv_map = batch_download(tickers, period="6mo")
    log.info(f"다운로드 완료: {len(ohlcv_map)}/{len(tickers)}종목")

    # 스코어링 (로컬 계산, 추가 API 호출 없음)
    results = []
    for ticker, sector in NDX100.items():
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
    send_slack(blocks)
    log.info("=== 완료 ===")


if __name__ == "__main__":
    main()
