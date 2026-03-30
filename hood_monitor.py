#!/usr/bin/env python3
"""
$HOOD Advanced Monitor v3.0
============================
Phase 1: 주가/뉴스 + 옵션 PCR + 공매도 잔고 + RSI/MACD + 내부자 매매 강화 + 위클리 브리핑
Phase 2: 13F 기관 포지션 추적
Phase 3: DCA 시그널 스코어 (Claude API 연동)

GitHub Actions 무료 티어 기반 서버리스 아키텍처
Slack Incoming Webhook으로 알림 전송
"""

import os
import sys
import json
import hashlib
import hmac
import time
import math
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field, asdict

import requests

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
TICKER = "HOOD"
CIK = "0001783879"  # Robinhood Markets CIK
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STATE_FILE = Path("state.json")
WEEKLY_STATE_FILE = Path("weekly_state.json")

# Yahoo Finance
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
YAHOO_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

# SEC EDGAR
SEC_HEADERS = {"User-Agent": "HoodMonitor/3.0 (personal-use)"}
EDGAR_FORM4_URL = "https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&dateRange=custom&startdt={start}&enddt={end}&forms=4"
EDGAR_FILING_URL = "https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&forms={form}&dateRange=custom&startdt={start}&enddt={end}"
EDGAR_FULL_TEXT_SEARCH = "https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&forms=4"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_13F_SEARCH = "https://efts.sec.gov/LATEST/search-index?q=%22HOOD%22&forms=13F-HR&dateRange=custom&startdt={start}&enddt={end}"

# FINRA / Short Interest
FINRA_SHORT_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"

# KST = UTC+9
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
    market_cap: str = ""
    timestamp: str = ""


@dataclass
class TechnicalSignals:
    rsi_14: float = 50.0
    macd_line: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    rsi_alert: str = ""  # "oversold" | "overbought" | ""
    macd_alert: str = ""  # "bullish_cross" | "bearish_cross" | ""


@dataclass
class OptionsData:
    pcr: float = 0.0  # Put/Call Ratio
    total_puts: int = 0
    total_calls: int = 0
    pcr_signal: str = ""  # "heavy_hedging" | "bullish" | "neutral"


@dataclass
class ShortInterestData:
    short_volume: int = 0
    total_volume: int = 0
    short_pct: float = 0.0
    date: str = ""
    signal: str = ""  # "high_short" | "normal"


@dataclass
class InsiderTrade:
    filer: str = ""
    title: str = ""
    trade_type: str = ""  # "Purchase" | "Sale"
    shares: int = 0
    price: float = 0.0
    total_value: float = 0.0
    date: str = ""
    url: str = ""


@dataclass
class Filing13F:
    institution: str = ""
    shares: int = 0
    value: float = 0.0  # in thousands
    change_type: str = ""  # "NEW" | "INCREASED" | "DECREASED" | "SOLD_ALL"
    change_pct: float = 0.0
    filing_date: str = ""
    url: str = ""


@dataclass
class DCASignal:
    score: int = 50  # 0~100
    summary: str = ""
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
        "last_price": 0,
        "last_news_hashes": [],
        "last_insider_hashes": [],
        "last_13f_hashes": [],
        "price_history": [],  # 최근 50일 종가 (RSI/MACD 계산용)
        "weekly_data": {
            "prices": [],
            "alerts": [],
            "insider_trades": [],
            "news_headlines": [],
        },
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
        "prices": [],
        "high": 0,
        "low": 999999,
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
def safe_get(url: str, headers: dict = None, params: dict = None, timeout: int = 15) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=headers or {}, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp
        log.warning(f"HTTP {resp.status_code} for {url}")
    except Exception as e:
        log.error(f"Request failed for {url}: {e}")
    return None


# ─────────────────────────────────────────────
# 1. 주가 데이터 (Yahoo Finance)
# ─────────────────────────────────────────────
def fetch_price() -> Optional[PriceData]:
    """Yahoo Finance에서 현재 주가 데이터 가져오기"""
    url = YAHOO_QUOTE_URL.format(ticker=TICKER)
    params = {"interval": "1d", "range": "5d"}
    resp = safe_get(url, params=params)
    if not resp:
        return None

    try:
        data = resp.json()
        result = data["chart"]["result"][0]
        meta = result["meta"]
        quotes = result["indicators"]["quote"][0]
        closes = [c for c in quotes["close"] if c is not None]

        current = meta.get("regularMarketPrice", closes[-1] if closes else 0)
        prev_close = meta.get("previousClose", meta.get("chartPreviousClose", 0))

        change_pct = ((current - prev_close) / prev_close * 100) if prev_close else 0

        p = PriceData(
            current=round(current, 2),
            prev_close=round(prev_close, 2),
            change_pct=round(change_pct, 2),
            high=round(max(q for q in quotes["high"] if q) if quotes["high"] else 0, 2),
            low=round(min(q for q in quotes["low"] if q) if quotes["low"] else 0, 2),
            volume=int(meta.get("regularMarketVolume", 0)),
            market_cap=format_market_cap(current * meta.get("sharesOutstanding", 0) if "sharesOutstanding" in meta else 0),
            timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        )
        return p
    except Exception as e:
        log.error(f"Price parse error: {e}")
        return None


def fetch_price_history(days: int = 60) -> list[float]:
    """RSI/MACD 계산을 위한 과거 종가 데이터"""
    url = YAHOO_QUOTE_URL.format(ticker=TICKER)
    params = {"interval": "1d", "range": f"{days}d"}
    resp = safe_get(url, params=params)
    if not resp:
        return []

    try:
        data = resp.json()
        quotes = data["chart"]["result"][0]["indicators"]["quote"][0]
        closes = [round(c, 2) for c in quotes["close"] if c is not None]
        return closes
    except Exception as e:
        log.error(f"Price history error: {e}")
        return []


def format_market_cap(val: float) -> str:
    if val >= 1e12:
        return f"${val/1e12:.1f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.1f}M"
    return f"${val:,.0f}"


# ─────────────────────────────────────────────
# 2. 뉴스 (Yahoo RSS)
# ─────────────────────────────────────────────
def fetch_news() -> list[dict]:
    """Yahoo Finance RSS에서 최신 뉴스 헤드라인"""
    url = YAHOO_RSS_URL.format(ticker=TICKER)
    resp = safe_get(url)
    if not resp:
        return []

    try:
        root = ET.fromstring(resp.text)
        items = root.findall(".//item")
        news = []
        for item in items[:10]:
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            news.append({
                "title": title,
                "link": link,
                "date": pub_date,
                "hash": hashlib.md5(title.encode()).hexdigest()[:12],
            })
        return news
    except Exception as e:
        log.error(f"News parse error: {e}")
        return []


# ─────────────────────────────────────────────
# 3. RSI / MACD 기술적 지표
# ─────────────────────────────────────────────
def calculate_rsi(closes: list[float], period: int = 14) -> float:
    """RSI(14) 계산"""
    if len(closes) < period + 1:
        return 50.0

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    # Wilder's smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calculate_macd(closes: list[float]) -> tuple[float, float, float]:
    """MACD(12,26,9) 계산 → (macd_line, signal_line, histogram)"""
    if len(closes) < 35:
        return 0.0, 0.0, 0.0

    def ema(data, period):
        k = 2 / (period + 1)
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(data[i] * k + result[-1] * (1 - k))
        return result

    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd_line = [ema12[i] - ema26[i] for i in range(len(closes))]
    signal_line = ema(macd_line, 9)
    histogram = macd_line[-1] - signal_line[-1]

    return round(macd_line[-1], 4), round(signal_line[-1], 4), round(histogram, 4)


def get_technical_signals(closes: list[float]) -> TechnicalSignals:
    """RSI와 MACD를 계산하고 알림 시그널 판단"""
    rsi = calculate_rsi(closes)
    macd_line, macd_signal, macd_hist = calculate_macd(closes)

    ts = TechnicalSignals(
        rsi_14=rsi,
        macd_line=macd_line,
        macd_signal=macd_signal,
        macd_histogram=macd_hist,
    )

    # RSI 알림
    if rsi <= 30:
        ts.rsi_alert = "oversold"
    elif rsi >= 70:
        ts.rsi_alert = "overbought"

    # MACD 크로스 감지 (최근 2개 값 비교)
    if len(closes) >= 36:
        closes_prev = closes[:-1]
        macd_prev, signal_prev, _ = calculate_macd(closes_prev)
        if macd_prev < signal_prev and macd_line > macd_signal:
            ts.macd_alert = "bullish_cross"
        elif macd_prev > signal_prev and macd_line < macd_signal:
            ts.macd_alert = "bearish_cross"

    return ts


# ─────────────────────────────────────────────
# 4. 옵션 Put/Call Ratio (CBOE)
# ─────────────────────────────────────────────
def fetch_options_pcr() -> Optional[OptionsData]:
    """CBOE 또는 Yahoo Options에서 PCR 계산"""
    # Yahoo Finance Options Chain 사용 (CBOE 직접 스크래핑보다 안정적)
    url = f"https://query1.finance.yahoo.com/v7/finance/options/{TICKER}"
    resp = safe_get(url)
    if not resp:
        return None

    try:
        data = resp.json()
        options = data["optionChain"]["result"][0]["options"]
        if not options:
            return None

        total_put_oi = 0
        total_call_oi = 0

        for chain in options:
            for put in chain.get("puts", []):
                total_put_oi += put.get("openInterest", 0)
            for call in chain.get("calls", []):
                total_call_oi += call.get("openInterest", 0)

        if total_call_oi == 0:
            return None

        pcr = total_put_oi / total_call_oi

        od = OptionsData(
            pcr=round(pcr, 3),
            total_puts=total_put_oi,
            total_calls=total_call_oi,
        )

        if pcr > 1.2:
            od.pcr_signal = "heavy_hedging"
        elif pcr < 0.5:
            od.pcr_signal = "bullish"
        else:
            od.pcr_signal = "neutral"

        return od
    except Exception as e:
        log.error(f"Options PCR error: {e}")
        return None


# ─────────────────────────────────────────────
# 5. 공매도 잔고 (FINRA RegSHO)
# ─────────────────────────────────────────────
def fetch_short_interest() -> Optional[ShortInterestData]:
    """FINRA RegSHO daily short volume 데이터"""
    # 최근 5영업일 시도 (주말/휴일 대비)
    now = datetime.now(UTC)
    for delta in range(0, 7):
        d = now - timedelta(days=delta)
        if d.weekday() >= 5:  # 주말 스킵
            continue
        date_str = d.strftime("%Y%m%d")
        url = FINRA_SHORT_URL.format(date=date_str)
        resp = safe_get(url)
        if resp and resp.text.strip():
            try:
                for line in resp.text.strip().split("\n"):
                    fields = line.split("|")
                    if len(fields) >= 5 and fields[1].upper() == TICKER:
                        short_vol = int(fields[2])
                        # fields[3] = short exempt volume
                        total_vol = int(fields[4])
                        short_pct = (short_vol / total_vol * 100) if total_vol > 0 else 0

                        sid = ShortInterestData(
                            short_volume=short_vol,
                            total_volume=total_vol,
                            short_pct=round(short_pct, 1),
                            date=d.strftime("%Y-%m-%d"),
                        )
                        sid.signal = "high_short" if short_pct > 50 else "normal"
                        return sid
            except Exception as e:
                log.error(f"Short interest parse error for {date_str}: {e}")
                continue
    return None


# ─────────────────────────────────────────────
# 6. 내부자 거래 강화 (SEC Form 4 XML 파싱)
# ─────────────────────────────────────────────
def fetch_insider_trades() -> list[InsiderTrade]:
    """SEC EDGAR Form 4 파싱 — 매수/매도 금액까지 추출"""
    trades = []
    try:
        # 최근 제출된 Form 4 목록 가져오기
        url = f"https://data.sec.gov/submissions/CIK{CIK}.json"
        resp = safe_get(url, headers=SEC_HEADERS)
        if not resp:
            return trades

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form != "4":
                continue
            if i >= 20:  # 최근 20개만 확인
                break

            accession = accessions[i].replace("-", "")
            filing_date = dates[i]

            # Form 4 XML 파싱
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{CIK.lstrip('0')}/{accession}/{primary_docs[i]}"
            xml_resp = safe_get(xml_url, headers=SEC_HEADERS)
            if not xml_resp:
                continue

            try:
                trade = parse_form4_xml(xml_resp.text, filing_date, xml_url)
                if trade:
                    trades.extend(trade)
            except Exception as e:
                log.warning(f"Form 4 XML parse failed: {e}")
                continue

            time.sleep(0.2)  # SEC rate limit 존중

    except Exception as e:
        log.error(f"Insider trades fetch error: {e}")

    return trades


def parse_form4_xml(xml_text: str, filing_date: str, url: str) -> list[InsiderTrade]:
    """Form 4 XML에서 거래 상세 파싱"""
    trades = []
    try:
        # XML 네임스페이스 제거
        xml_clean = xml_text
        for ns in ['xmlns="http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"',
                    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"']:
            xml_clean = xml_clean.replace(ns, "")

        root = ET.fromstring(xml_clean)

        # 보고자 정보
        reporter = root.find(".//reportingOwner/reportingOwnerId")
        if reporter is None:
            reporter = root.find(".//reportingOwnerId")

        filer_name = ""
        filer_title = ""
        if reporter is not None:
            filer_name = reporter.findtext("rptOwnerName", "Unknown")
        
        relationship = root.find(".//reportingOwner/reportingOwnerRelationship")
        if relationship is None:
            relationship = root.find(".//reportingOwnerRelationship")
        if relationship is not None:
            filer_title = relationship.findtext("officerTitle", "")

        # Non-derivative 거래
        for txn in root.findall(".//nonDerivativeTransaction"):
            trade = _parse_transaction(txn, filer_name, filer_title, filing_date, url)
            if trade:
                trades.append(trade)

        # Derivative 거래
        for txn in root.findall(".//derivativeTransaction"):
            trade = _parse_transaction(txn, filer_name, filer_title, filing_date, url, derivative=True)
            if trade:
                trades.append(trade)

    except ET.ParseError:
        log.warning("XML parse error — possibly HTML response")
    except Exception as e:
        log.warning(f"Form 4 detail parse error: {e}")

    return trades


def _parse_transaction(txn, filer_name, filer_title, filing_date, url, derivative=False):
    """개별 거래 항목 파싱"""
    try:
        amounts = txn.find("transactionAmounts") if not derivative else txn.find("transactionAmounts")
        if amounts is None:
            return None

        shares_elem = amounts.find("transactionShares/value")
        price_elem = amounts.find("transactionPricePerShare/value")
        code_elem = amounts.find("transactionAcquiredDisposedCode/value")

        shares = float(shares_elem.text) if shares_elem is not None and shares_elem.text else 0
        price = float(price_elem.text) if price_elem is not None and price_elem.text else 0
        acq_disp = code_elem.text if code_elem is not None else ""

        trade_type = "Purchase" if acq_disp == "A" else "Sale" if acq_disp == "D" else "Other"
        total_value = shares * price

        if shares == 0:
            return None

        return InsiderTrade(
            filer=filer_name,
            title=filer_title,
            trade_type=trade_type,
            shares=int(shares),
            price=round(price, 2),
            total_value=round(total_value, 2),
            date=filing_date,
            url=url,
        )
    except Exception:
        return None


# ─────────────────────────────────────────────
# 7. 13F 기관 포지션 추적 (SEC EDGAR)
# ─────────────────────────────────────────────
def fetch_13f_filings() -> list[Filing13F]:
    """최근 13F-HR 파일링에서 HOOD 포지션 추적"""
    filings = []
    try:
        # EDGAR Full-Text Search로 HOOD 멘션된 13F 찾기
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
        start_date = (datetime.now(UTC) - timedelta(days=120)).strftime("%Y-%m-%d")  # 최근 4개월

        search_url = f"https://efts.sec.gov/LATEST/search-index?q=%22HOOD%22+%22Robinhood+Markets%22&forms=13F-HR&dateRange=custom&startdt={start_date}&enddt={end_date}"
        resp = safe_get(search_url, headers=SEC_HEADERS)
        if not resp:
            # 대안: EDGAR full text search API
            search_url2 = f"https://efts.sec.gov/LATEST/search-index?q=%22Robinhood%22&forms=13F-HR&dateRange=custom&startdt={start_date}&enddt={end_date}"
            resp = safe_get(search_url2, headers=SEC_HEADERS)

        if not resp:
            return filings

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])

        for hit in hits[:15]:  # 최근 15개 기관
            source = hit.get("_source", {})
            entity = source.get("entity_name", source.get("display_names", ["Unknown"])[0] if source.get("display_names") else "Unknown")
            filing_date = source.get("file_date", "")
            accession = source.get("accession_no", "")

            if not accession:
                continue

            # 13F XML 파싱은 복잡하므로 메타 정보만 수집
            filing = Filing13F(
                institution=entity if isinstance(entity, str) else str(entity),
                filing_date=filing_date,
                url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&accession={accession}&type=13F-HR&dateb=&owner=include&count=1",
            )
            filings.append(filing)
            time.sleep(0.15)

    except Exception as e:
        log.error(f"13F fetch error: {e}")

    return filings


# ─────────────────────────────────────────────
# 8. Phase 3: DCA 시그널 스코어 (Claude API)
# ─────────────────────────────────────────────
def calculate_dca_signal(
    price: PriceData,
    technicals: TechnicalSignals,
    options: Optional[OptionsData],
    short_interest: Optional[ShortInterestData],
    insider_trades: list[InsiderTrade],
    news: list[dict],
) -> DCASignal:
    """
    Claude API를 사용해 DCA 추가매수 환경 스코어 산출 (0~100)
    API 키가 없으면 규칙 기반 fallback
    """
    # 먼저 규칙 기반 점수 계산 (fallback이자 기본값)
    score, factors = _rule_based_dca_score(price, technicals, options, short_interest, insider_trades)

    # Claude API 사용 가능하면 AI 분석 추가
    if ANTHROPIC_API_KEY:
        try:
            ai_signal = _claude_dca_analysis(price, technicals, options, short_interest, insider_trades, news, score, factors)
            if ai_signal:
                return ai_signal
        except Exception as e:
            log.warning(f"Claude API error, using rule-based fallback: {e}")

    return DCASignal(score=score, summary=_score_to_summary(score), factors=factors)


def _rule_based_dca_score(price, technicals, options, short_interest, insider_trades) -> tuple[int, list]:
    """규칙 기반 DCA 스코어 (Claude API fallback)"""
    score = 50  # 중립 시작
    factors = []

    # RSI 반영 (최대 ±20)
    if technicals.rsi_14 <= 30:
        bonus = min(20, int((30 - technicals.rsi_14) * 1.5))
        score += bonus
        factors.append(f"🟢 RSI {technicals.rsi_14} — 과매도 구간 (+{bonus})")
    elif technicals.rsi_14 <= 40:
        score += 8
        factors.append(f"🟡 RSI {technicals.rsi_14} — 매수 관심 구간 (+8)")
    elif technicals.rsi_14 >= 70:
        penalty = min(15, int((technicals.rsi_14 - 70) * 1.0))
        score -= penalty
        factors.append(f"🔴 RSI {technicals.rsi_14} — 과매수 구간 (-{penalty})")

    # MACD 반영 (최대 ±10)
    if technicals.macd_alert == "bullish_cross":
        score += 10
        factors.append("🟢 MACD 골든크로스 (+10)")
    elif technicals.macd_alert == "bearish_cross":
        score -= 10
        factors.append("🔴 MACD 데드크로스 (-10)")
    elif technicals.macd_histogram > 0:
        score += 3
        factors.append("🟡 MACD 히스토그램 양전환 (+3)")

    # PCR 반영 (최대 ±10)
    if options:
        if options.pcr > 1.2:
            score += 8  # 공포 = 역발상 매수
            factors.append(f"🟢 PCR {options.pcr:.2f} — 과도한 풋 헤징, 역발상 매수 기회 (+8)")
        elif options.pcr > 1.0:
            score += 3
            factors.append(f"🟡 PCR {options.pcr:.2f} — 약간의 헤징 (+3)")
        elif options.pcr < 0.5:
            score -= 5
            factors.append(f"🔴 PCR {options.pcr:.2f} — 과도한 낙관 (-5)")

    # 공매도 반영 (최대 ±8)
    if short_interest:
        if short_interest.short_pct > 60:
            score += 8  # 극단적 공매도 = 숏스퀴즈 가능
            factors.append(f"🟢 공매도 비율 {short_interest.short_pct:.1f}% — 숏스퀴즈 가능성 (+8)")
        elif short_interest.short_pct > 50:
            score += 3
            factors.append(f"🟡 공매도 비율 {short_interest.short_pct:.1f}% — 높은 편 (+3)")
        elif short_interest.short_pct < 20:
            score -= 3
            factors.append(f"🟡 공매도 비율 {short_interest.short_pct:.1f}% — 낮음 (-3)")

    # 내부자 매매 반영 (최대 ±10)
    recent_buys = sum(1 for t in insider_trades if t.trade_type == "Purchase" and _is_recent(t.date, 30))
    recent_sells = sum(1 for t in insider_trades if t.trade_type == "Sale" and _is_recent(t.date, 30))
    buy_value = sum(t.total_value for t in insider_trades if t.trade_type == "Purchase" and _is_recent(t.date, 30))
    sell_value = sum(t.total_value for t in insider_trades if t.trade_type == "Sale" and _is_recent(t.date, 30))

    if recent_buys > 0 and buy_value > 100000:
        score += 10
        factors.append(f"🟢 내부자 매수 {recent_buys}건, ${buy_value:,.0f} (+10)")
    elif recent_sells > 2 and sell_value > 1000000:
        score -= 8
        factors.append(f"🔴 내부자 매도 {recent_sells}건, ${sell_value:,.0f} (-8)")

    # 가격 변동 반영 (최대 ±5)
    if price.change_pct <= -5:
        score += 5
        factors.append(f"🟢 큰 폭 하락 {price.change_pct:+.1f}% — 매수 기회 (+5)")
    elif price.change_pct >= 5:
        score -= 3
        factors.append(f"🟡 큰 폭 상승 {price.change_pct:+.1f}% — 추격 매수 주의 (-3)")

    score = max(0, min(100, score))
    return score, factors


def _claude_dca_analysis(price, technicals, options, short_interest, insider_trades, news, rule_score, rule_factors) -> Optional[DCASignal]:
    """Claude API로 종합 DCA 분석"""
    context = f"""
당신은 $HOOD (Robinhood Markets) 장기 DCA 투자자를 위한 시그널 분석가입니다.
아래 데이터를 종합하여 "지금 DCA 추가매수 환경인가"를 0~100 점수로 평가해주세요.

점수 기준:
- 0~20: 매수 자제 권장 (과매수, 악재 집중)
- 20~40: 관망
- 40~60: 정기 DCA 유지
- 60~80: DCA 추가매수 우호적 환경
- 80~100: 강력한 추가매수 시그널 (과매도, 내부자 매수 등)

현재 데이터:
- 주가: ${price.current} ({price.change_pct:+.1f}%)
- RSI(14): {technicals.rsi_14}
- MACD: {technicals.macd_line:.4f} (Signal: {technicals.macd_signal:.4f}, Hist: {technicals.macd_histogram:.4f})
- MACD 알림: {technicals.macd_alert or "없음"}
- 옵션 PCR: {options.pcr:.3f if options else "N/A"} (풋 {options.total_puts:,} / 콜 {options.total_calls:,} if options else "")
- 공매도 비율: {short_interest.short_pct:.1f}% if short_interest else "N/A"
- 최근 내부자 매수: {sum(1 for t in insider_trades if t.trade_type == "Purchase")}건
- 최근 내부자 매도: {sum(1 for t in insider_trades if t.trade_type == "Sale")}건
- 규칙 기반 점수: {rule_score}/100

규칙 기반 분석 요인:
{chr(10).join(rule_factors)}

최근 뉴스 헤드라인:
{chr(10).join(f"- {n['title']}" for n in news[:5])}

다음 JSON 형식으로만 응답해주세요:
{{"score": 정수(0-100), "summary": "한국어 2문장 요약", "factors": ["핵심 요인1", "핵심 요인2", "핵심 요인3"]}}
"""

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": context}],
        },
        timeout=30,
    )

    if resp.status_code != 200:
        log.warning(f"Claude API HTTP {resp.status_code}")
        return None

    data = resp.json()
    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text += block["text"]

    # JSON 파싱
    text = text.strip().replace("```json", "").replace("```", "").strip()
    result = json.loads(text)

    return DCASignal(
        score=max(0, min(100, int(result.get("score", rule_score)))),
        summary=result.get("summary", _score_to_summary(rule_score)),
        factors=result.get("factors", rule_factors),
    )


def _score_to_summary(score: int) -> str:
    if score >= 80:
        return "강력한 추가매수 시그널. 다수의 지표가 매수 우호적 환경을 가리키고 있습니다."
    elif score >= 60:
        return "DCA 추가매수를 고려할 만한 환경. 일부 긍정적 시그널이 감지됩니다."
    elif score >= 40:
        return "정기 DCA 유지 적절. 특별한 추가매수 시그널은 없습니다."
    elif score >= 20:
        return "관망 권장. 부정적 시그널이 다소 우세합니다."
    else:
        return "매수 자제 권장. 다수의 부정적 시그널이 감지됩니다."


def _is_recent(date_str: str, days: int) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return (datetime.now() - d).days <= days
    except Exception:
        return False


# ─────────────────────────────────────────────
# Slack 메시지 포맷터
# ─────────────────────────────────────────────
def format_price_block(price: PriceData) -> dict:
    emoji = "🟢" if price.change_pct >= 0 else "🔴"
    arrow = "▲" if price.change_pct >= 0 else "▼"
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*${TICKER} 주가 업데이트* {emoji}\n"
                f"*${price.current}* ({arrow} {abs(price.change_pct):.2f}%)\n"
                f"전일 종가: ${price.prev_close} | 고가: ${price.high} | 저가: ${price.low}\n"
                f"거래량: {price.volume:,}"
                + (f" | 시가총액: {price.market_cap}" if price.market_cap else "")
            ),
        },
    }


def format_technicals_block(ts: TechnicalSignals) -> dict:
    rsi_emoji = "🟢" if ts.rsi_alert == "oversold" else "🔴" if ts.rsi_alert == "overbought" else "⚪"
    macd_emoji = "🟢" if ts.macd_alert == "bullish_cross" else "🔴" if ts.macd_alert == "bearish_cross" else "⚪"

    alerts = []
    if ts.rsi_alert == "oversold":
        alerts.append("⚠️ *RSI 과매도 — DCA 타이밍 체크!*")
    elif ts.rsi_alert == "overbought":
        alerts.append("⚠️ *RSI 과매수 — 추격 매수 주의*")
    if ts.macd_alert == "bullish_cross":
        alerts.append("⚠️ *MACD 골든크로스 발생!*")
    elif ts.macd_alert == "bearish_cross":
        alerts.append("⚠️ *MACD 데드크로스 발생*")

    text = (
        f"*📊 기술적 지표*\n"
        f"{rsi_emoji} RSI(14): *{ts.rsi_14}*\n"
        f"{macd_emoji} MACD: {ts.macd_line:.4f} | Signal: {ts.macd_signal:.4f} | Hist: {ts.macd_histogram:.4f}"
    )
    if alerts:
        text += "\n" + "\n".join(alerts)

    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def format_options_block(options: OptionsData) -> dict:
    signal_text = {
        "heavy_hedging": "🟡 풋 헤징 집중 (역발상 매수 시그널)",
        "bullish": "🟢 콜 우세 (낙관적)",
        "neutral": "⚪ 중립",
    }
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*📈 옵션 시장*\n"
                f"Put/Call Ratio: *{options.pcr:.3f}* — {signal_text.get(options.pcr_signal, '⚪')}\n"
                f"풋 OI: {options.total_puts:,} | 콜 OI: {options.total_calls:,}"
            ),
        },
    }


def format_short_block(si: ShortInterestData) -> dict:
    emoji = "🔴" if si.signal == "high_short" else "⚪"
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*🩳 공매도 현황* ({si.date})\n"
                f"{emoji} 공매도 비율: *{si.short_pct:.1f}%*\n"
                f"공매도 거래량: {si.short_volume:,} / 총: {si.total_volume:,}"
            ),
        },
    }


def format_insider_block(trades: list[InsiderTrade]) -> list[dict]:
    if not trades:
        return []

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*🕴️ 내부자 거래*"}}]

    for t in trades[:5]:  # 최대 5건
        emoji = "🟢" if t.trade_type == "Purchase" else "🔴"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{t.filer}* ({t.title})\n"
                    f"{t.trade_type}: {t.shares:,}주 @ ${t.price:.2f} "
                    f"= *${t.total_value:,.0f}*\n"
                    f"📅 {t.date} | <{t.url}|SEC Filing>"
                ),
            },
        })

    return blocks


def format_13f_block(filings: list[Filing13F]) -> list[dict]:
    if not filings:
        return []

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*🏛️ 13F 기관 포지션 변동*"}}]

    for f in filings[:8]:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"📋 *{f.institution}*\n"
                    f"제출일: {f.filing_date} | <{f.url}|SEC Filing>"
                ),
            },
        })

    return blocks


def format_news_block(news: list[dict]) -> list[dict]:
    if not news:
        return []

    items = "\n".join(f"• <{n['link']}|{n['title']}>" for n in news[:5])
    return [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*📰 최신 뉴스*\n{items}"},
    }]


def format_dca_block(signal: DCASignal) -> dict:
    bar = _score_bar(signal.score)
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*🎯 DCA 시그널 스코어: {signal.score}/100*\n"
                f"{bar}\n"
                f"{signal.summary}\n"
                + ("\n".join(f"  • {f}" for f in signal.factors[:5]) if signal.factors else "")
            ),
        },
    }


def _score_bar(score: int) -> str:
    filled = score // 5
    empty = 20 - filled
    if score >= 70:
        color = "🟩"
    elif score >= 40:
        color = "🟨"
    else:
        color = "🟥"
    return color * filled + "⬜" * empty


# ─────────────────────────────────────────────
# Slack 전송
# ─────────────────────────────────────────────
def send_slack(blocks: list[dict], text: str = "HOOD Monitor Alert"):
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK_URL not set — printing to stdout")
        for b in blocks:
            if "text" in b and isinstance(b["text"], dict):
                print(b["text"].get("text", ""))
            elif "text" in b:
                print(b["text"])
        return

    payload = {"text": text, "blocks": blocks}
    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        if resp.status_code != 200:
            log.error(f"Slack send failed: {resp.status_code} {resp.text}")
        else:
            log.info("Slack message sent successfully")
    except Exception as e:
        log.error(f"Slack send error: {e}")


# ─────────────────────────────────────────────
# 실행 모드
# ─────────────────────────────────────────────
def run_normal():
    """일반 모드: 주가 + 뉴스 + RSI/MACD + 내부자 (매시간)"""
    log.info("=== NORMAL mode ===")
    state = load_state()
    ws = load_weekly_state()
    blocks = []

    # 주가
    price = fetch_price()
    if price:
        blocks.append(format_price_block(price))

        # 가격 변동 알림 (5% 이상)
        if state["last_price"] > 0:
            delta = abs(price.current - state["last_price"]) / state["last_price"] * 100
            if delta >= 5:
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"🚨 *주가 급변동!* 전 체크 대비 {delta:.1f}% 변동"},
                })

        state["last_price"] = price.current
        ws.setdefault("prices", []).append(price.current)
        ws["high"] = max(ws.get("high", 0), price.current)
        if ws.get("low", 999999) == 999999 or price.current < ws["low"]:
            ws["low"] = price.current

    # RSI / MACD
    closes = fetch_price_history(60)
    if closes:
        state["price_history"] = closes[-60:]
        technicals = get_technical_signals(closes)
        blocks.append(format_technicals_block(technicals))
        ws.setdefault("rsi_readings", []).append(technicals.rsi_14)

        # RSI 알림 (과매도 시 중요)
        if technicals.rsi_alert == "oversold":
            ws.setdefault("alerts_fired", []).append(f"RSI {technicals.rsi_14} 과매도")
        if technicals.macd_alert:
            ws.setdefault("alerts_fired", []).append(f"MACD {technicals.macd_alert}")
    else:
        technicals = TechnicalSignals()

    # 뉴스
    news = fetch_news()
    new_news = [n for n in news if n["hash"] not in state.get("last_news_hashes", [])]
    if new_news:
        blocks.extend(format_news_block(new_news))
        state["last_news_hashes"] = [n["hash"] for n in news[:20]]
        for n in new_news[:3]:
            ws.setdefault("news_headlines", []).append(n["title"])

    # 내부자 거래
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
                f"{t.trade_type}: {t.filer} {t.shares:,}주 ${t.total_value:,.0f}"
            )

    if blocks:
        blocks.insert(0, {"type": "divider"})
        blocks.insert(0, {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 $HOOD Monitor — {datetime.now(KST).strftime('%m/%d %H:%M KST')}"},
        })
        blocks.append({"type": "divider"})
        send_slack(blocks)

    save_state(state)
    save_weekly_state(ws)
    log.info("Normal mode complete")


def run_close():
    """장 마감 모드: 옵션 PCR + 공매도 잔고 + DCA 시그널 (매일 장 마감 후)"""
    log.info("=== CLOSE mode ===")
    state = load_state()
    ws = load_weekly_state()
    blocks = []

    # 주가
    price = fetch_price()
    if price:
        blocks.append(format_price_block(price))
        state["last_price"] = price.current

    # 기술적 지표
    closes = fetch_price_history(60)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    blocks.append(format_technicals_block(technicals))

    # 옵션 PCR
    options = fetch_options_pcr()
    if options:
        blocks.append(format_options_block(options))
        ws.setdefault("pcr_readings", []).append(options.pcr)

    # 공매도
    short = fetch_short_interest()
    if short:
        blocks.append(format_short_block(short))
        ws.setdefault("short_readings", []).append(short.short_pct)

    # 내부자 거래
    insider_trades = fetch_insider_trades()
    new_insiders = [t for t in insider_trades
                    if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                    not in state.get("last_insider_hashes", [])]
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))

    # 뉴스
    news = fetch_news()

    # DCA 시그널 (Phase 3)
    dca = calculate_dca_signal(price or PriceData(), technicals, options, short, insider_trades, news)
    blocks.append(format_dca_block(dca))

    if blocks:
        blocks.insert(0, {"type": "divider"})
        blocks.insert(0, {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🔔 $HOOD 장 마감 리포트 — {datetime.now(KST).strftime('%m/%d')}"},
        })
        blocks.append({"type": "divider"})
        send_slack(blocks)

    save_state(state)
    save_weekly_state(ws)
    log.info("Close mode complete")


def run_13f():
    """13F 모드: 기관 포지션 추적 (주 1회)"""
    log.info("=== 13F mode ===")
    state = load_state()

    filings = fetch_13f_filings()
    new_filings = [f for f in filings
                   if hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
                   not in state.get("last_13f_hashes", [])]

    if new_filings:
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🏛️ $HOOD 13F 기관 포지션 업데이트"},
            },
            {"type": "divider"},
        ]
        blocks.extend(format_13f_block(new_filings))
        blocks.append({"type": "divider"})
        send_slack(blocks)

        state["last_13f_hashes"] = [
            hashlib.md5(f"{f.institution}{f.filing_date}".encode()).hexdigest()[:12]
            for f in filings[:30]
        ]
        save_state(state)

    log.info(f"13F mode complete — {len(new_filings)} new filings")


def run_weekly():
    """위클리 브리핑: 매주 월요일 아침 종합 리포트 + DCA 시그널"""
    log.info("=== WEEKLY BRIEFING mode ===")
    ws = load_weekly_state()

    # 주가 현재 정보
    price = fetch_price()
    closes = fetch_price_history(60)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    options = fetch_options_pcr()
    short = fetch_short_interest()
    insider_trades = fetch_insider_trades()
    news = fetch_news()

    # DCA 시그널
    dca = calculate_dca_signal(price or PriceData(), technicals, options, short, insider_trades, news)

    # 주간 요약 빌드
    prices = ws.get("prices", [])
    week_high = ws.get("high", 0)
    week_low = ws.get("low", 0)
    alerts = ws.get("alerts_fired", [])
    insider_summary = ws.get("insider_trades", [])
    news_summary = ws.get("news_headlines", [])
    rsi_readings = ws.get("rsi_readings", [])
    pcr_readings = ws.get("pcr_readings", [])
    short_readings = ws.get("short_readings", [])

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📋 $HOOD 주간 브리핑 — {datetime.now(KST).strftime('%m/%d')} (월)"},
        },
        {"type": "divider"},
    ]

    # 주가 요약
    if price:
        week_change = ""
        if prices and len(prices) >= 2:
            wk_chg = (prices[-1] - prices[0]) / prices[0] * 100
            week_change = f" | 주간 변동: {wk_chg:+.1f}%"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*💰 주가 현황*\n"
                    f"현재: *${price.current}* ({price.change_pct:+.1f}%){week_change}\n"
                    f"주간 고가: ${week_high:.2f} | 주간 저가: ${week_low:.2f}"
                ),
            },
        })

    # 기술적 지표 요약
    blocks.append(format_technicals_block(technicals))

    # 옵션/공매도 요약
    if pcr_readings:
        avg_pcr = sum(pcr_readings) / len(pcr_readings)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📈 주간 옵션 PCR 평균: {avg_pcr:.3f}*" + (f" | 현재: {options.pcr:.3f}" if options else ""),
            },
        })

    if short_readings:
        avg_short = sum(short_readings) / len(short_readings)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🩳 주간 공매도 비율 평균: {avg_short:.1f}%*" + (f" | 최신: {short.short_pct:.1f}%" if short else ""),
            },
        })

    # 주간 알림 요약
    if alerts:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🚨 주간 알림 발동*\n" + "\n".join(f"• {a}" for a in alerts[-10:]),
            },
        })

    # 내부자 거래 요약
    if insider_summary:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🕴️ 주간 내부자 거래*\n" + "\n".join(f"• {t}" for t in insider_summary[-5:]),
            },
        })

    # 뉴스 요약
    if news_summary:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📰 주간 주요 뉴스*\n" + "\n".join(f"• {h}" for h in news_summary[-5:]),
            },
        })

    # DCA 시그널 (Phase 3)
    blocks.append({"type": "divider"})
    blocks.append(format_dca_block(dca))
    blocks.append({"type": "divider"})

    send_slack(blocks)

    # 위클리 상태 초기화
    save_weekly_state({
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "prices": [],
        "high": 0,
        "low": 999999,
        "alerts_fired": [],
        "insider_trades": [],
        "news_headlines": [],
        "rsi_readings": [],
        "pcr_readings": [],
        "short_readings": [],
    })

    log.info("Weekly briefing complete")


# ─────────────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────────────
def main():
    mode = os.environ.get("RUN_MODE", sys.argv[1] if len(sys.argv) > 1 else "normal").lower()

    log.info(f"Starting HOOD Monitor v3.0 — mode: {mode}")
    log.info(f"Time: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}")

    if mode == "normal":
        run_normal()
    elif mode == "close":
        run_close()
    elif mode == "13f":
        run_13f()
    elif mode == "weekly":
        run_weekly()
    else:
        log.error(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
