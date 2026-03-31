#!/usr/bin/env python3
"""
$HOOD Advanced Monitor v3.1
============================
BUG FIXES from v3.0:
  1. run_morning() 종가 기준 누적 오류 → state 기반으로 변경
  2. 뉴스 HOOD 관련성 필터 + 한국어 강제
  3. 13F EDGAR API URL 수정 + infoTable 주식 수/금액 파싱
  4. weekly 가격 숫자 노출 제거 + DCA 결론 명확화
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
        # ── BUG 1 FIX: run_close()가 여기 저장, run_morning()이 읽음 ──
        "pending_morning_alert": None,
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
def fetch_price() -> Optional[PriceData]:
    _yahoo_throttle()
    url = YAHOO_QUOTE_URL.format(ticker=TICKER)
    resp = safe_get(url, params={"interval": "1d", "range": "5d"})
    if not resp:
        return None
    try:
        data = resp.json()
        result = data["chart"]["result"][0]
        meta = result["meta"]
        quotes = result["indicators"]["quote"][0]
        closes = [c for c in quotes["close"] if c is not None]

        prev_close = closes[-2] if len(closes) >= 2 else 0
        current = closes[-1] if closes else 0
        change_pct = ((current - prev_close) / prev_close * 100) if prev_close else 0

        return PriceData(
            current=round(current, 2),
            prev_close=round(prev_close, 2),
            change_pct=round(change_pct, 2),
            high=round(max(q for q in quotes["high"] if q), 2) if any(quotes["high"]) else 0,
            low=round(min(q for q in quotes["low"] if q), 2) if any(quotes["low"]) else 0,
            volume=int(meta.get("regularMarketVolume", 0)),
            timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        )
    except Exception as e:
        log.error(f"Price parse error: {e}")
        return None


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
            title = item.findtext("title", "")
            pub_date = item.findtext("pubDate", "")
            news.append({
                "title": title,
                "date": pub_date,
                "hash": hashlib.md5(title.encode()).hexdigest()[:12],
            })
        return news
    except Exception as e:
        log.error(f"News parse error: {e}")
        return []


def translate_news(news: list) -> list:
    """
    BUG 2 FIX:
    - HOOD/Robinhood 직접 관련 뉴스만 처리
    - 출력 언어를 한국어로 강제
    - 관련 없으면 skip 표시
    """
    if not news or not ANTHROPIC_API_KEY:
        return news

    titles = "\n".join(f"{i+1}. {n['title']}" for i, n in enumerate(news))
    prompt = f"""당신은 $HOOD(Robinhood Markets) 투자 알림 봇입니다.
아래 뉴스 헤드라인 목록을 분석해주세요.

규칙:
1. Robinhood Markets / $HOOD 주가에 직접 영향을 주는 뉴스만 포함 (간접 연관 제외)
2. 포함 기준: 실적, 규제, 경쟁사 직접 비교, 경영진 변동, 주요 제품/서비스, 기관 매수/매도, 소송
3. 제외 기준: 증권업 일반 뉴스, 금리 일반론, 다른 회사 뉴스에 HOOD가 언급만 된 경우
4. 반드시 한국어로만 출력

각 뉴스에 대해 JSON 배열로만 응답 (다른 텍스트 없이):
[
  {{"idx": 1, "relevant": true, "summary": "15자 이내 한국어 요약", "sentiment": "positive|negative|neutral"}},
  {{"idx": 2, "relevant": false}}
]

뉴스 목록:
{titles}"""

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
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return news

        text = ""
        for block in resp.json().get("content", []):
            if block.get("type") == "text":
                text += block["text"]
        text = text.strip().replace("```json", "").replace("```", "").strip()
        results = json.loads(text)

        for item in results:
            idx = item.get("idx", 0) - 1
            if not (0 <= idx < len(news)):
                continue
            if not item.get("relevant", False):
                news[idx]["skip"] = True
            else:
                news[idx]["summary"] = item.get("summary", "")
                news[idx]["sentiment"] = item.get("sentiment", "neutral")
    except Exception as e:
        log.warning(f"News translation error: {e}")

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
# 4. 옵션 PCR
# ─────────────────────────────────────────────
def fetch_options_pcr() -> Optional[OptionsData]:
    _yahoo_throttle()
    url = f"https://query1.finance.yahoo.com/v7/finance/options/{TICKER}"
    resp = safe_get(url)
    if not resp:
        return None
    try:
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
        log.error(f"PCR error: {e}")
        return None


# ─────────────────────────────────────────────
# 5. 공매도
# ─────────────────────────────────────────────
def fetch_short_interest() -> Optional[ShortInterestData]:
    now = datetime.now(UTC)
    for delta in range(0, 7):
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
                    short_vol = int(fields[2])
                    total_vol = int(fields[4])
                    short_pct = (short_vol / total_vol * 100) if total_vol > 0 else 0
                    sid = ShortInterestData(
                        short_volume=short_vol, total_volume=total_vol,
                        short_pct=round(short_pct, 1), date=d.strftime("%Y-%m-%d"),
                    )
                    sid.signal = "high_short" if short_pct > 50 else "normal"
                    return sid
        except Exception as e:
            log.error(f"Short interest parse error: {e}")
    return None


# ─────────────────────────────────────────────
# 6. 내부자 거래 (Form 4)
# ─────────────────────────────────────────────
def fetch_insider_trades() -> list:
    trades = []
    try:
        url = f"https://data.sec.gov/submissions/CIK{CIK_PADDED}.json"
        resp = safe_get(url, headers=SEC_HEADERS)
        if not resp:
            return trades

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        checked = 0
        for i, form in enumerate(forms):
            if form != "4":
                continue
            if checked >= 20:
                break
            checked += 1

            acc = accessions[i].replace("-", "")
            filing_date = dates[i]
            xml_resp = None

            # index.json에서 원본 XML 탐색
            idx_url = f"https://data.sec.gov/Archives/edgar/data/{CIK_SHORT}/{acc}/index.json"
            idx_resp = safe_get(idx_url, headers=SEC_HEADERS, retries=1)
            if idx_resp:
                try:
                    for item in idx_resp.json().get("directory", {}).get("item", []):
                        name = item.get("name", "")
                        if (name.endswith(".xml")
                                and "xsl" not in name.lower()
                                and not name.startswith("R")
                                and "Financial" not in name):
                            xml_url = f"https://data.sec.gov/Archives/edgar/data/{CIK_SHORT}/{acc}/{name}"
                            xml_resp = safe_get(xml_url, headers=SEC_HEADERS, retries=1)
                            if xml_resp:
                                break
                except Exception:
                    pass

            if not xml_resp:
                # fallback: primaryDocument 파일명 직접 사용
                prim = primary_docs[i].split("/")[-1]
                xml_url = f"https://data.sec.gov/Archives/edgar/data/{CIK_SHORT}/{acc}/{prim}"
                xml_resp = safe_get(xml_url, headers=SEC_HEADERS, retries=1)

            if xml_resp:
                try:
                    trades.extend(parse_form4_xml(xml_resp.text, filing_date, xml_url))
                except Exception as e:
                    log.warning(f"Form 4 parse error: {e}")

            time.sleep(0.2)

    except Exception as e:
        log.error(f"Insider fetch error: {e}")
    return trades


def parse_form4_xml(xml_text: str, filing_date: str, url: str) -> list:
    trades = []
    try:
        xml_clean = xml_text
        for ns in ['xmlns="http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"',
                   'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"']:
            xml_clean = xml_clean.replace(ns, "")
        root = ET.fromstring(xml_clean)

        reporter = root.find(".//reportingOwner/reportingOwnerId") or root.find(".//reportingOwnerId")
        filer_name = reporter.findtext("rptOwnerName", "Unknown") if reporter else "Unknown"
        rel = root.find(".//reportingOwner/reportingOwnerRelationship") or root.find(".//reportingOwnerRelationship")
        filer_title = rel.findtext("officerTitle", "") if rel else ""

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
        amounts = txn.find("transactionAmounts")
        if amounts is None:
            return None
        shares_e = amounts.find("transactionShares/value")
        price_e = amounts.find("transactionPricePerShare/value")
        code_e = amounts.find("transactionAcquiredDisposedCode/value")
        shares = float(shares_e.text) if shares_e is not None and shares_e.text else 0
        price = float(price_e.text) if price_e is not None and price_e.text else 0
        acq = code_e.text if code_e is not None else ""
        if shares == 0:
            return None
        trade_type = "Purchase" if acq == "A" else "Sale" if acq == "D" else "Other"
        return InsiderTrade(
            filer=filer_name, title=filer_title, trade_type=trade_type,
            shares=int(shares), price=round(price, 2),
            total_value=round(shares * price, 2),
            date=filing_date, url=url,
        )
    except Exception:
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
def calculate_dca_signal(price, technicals, options, short_interest, insider_trades, news) -> DCASignal:
    score, factors = _rule_based_dca_score(price, technicals, options, short_interest, insider_trades)

    if ANTHROPIC_API_KEY:
        try:
            ai = _claude_dca_analysis(technicals, options, short_interest, insider_trades, news, score, factors)
            if ai:
                return ai
        except Exception as e:
            log.warning(f"Claude DCA fallback: {e}")

    return DCASignal(
        score=score,
        verdict=_score_to_verdict(score),
        summary=_score_to_summary(score),
        factors=factors,
    )


def _rule_based_dca_score(price, technicals, options, short_interest, insider_trades):
    score = 50
    factors = []

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

    if technicals.macd_alert == "bullish_cross":
        score += 10
        factors.append("🟢 MACD 골든크로스 (+10)")
    elif technicals.macd_alert == "bearish_cross":
        score -= 10
        factors.append("🔴 MACD 데드크로스 (-10)")

    if options:
        if options.pcr > 1.2:
            score += 8
            factors.append(f"🟢 PCR {options.pcr:.2f} 과도한 풋 헤징 → 역발상 매수 (+8)")
        elif options.pcr < 0.5:
            score -= 5
            factors.append(f"🔴 PCR {options.pcr:.2f} 과도한 낙관 (-5)")

    if short_interest:
        if short_interest.short_pct > 60:
            score += 8
            factors.append(f"🟢 공매도 {short_interest.short_pct:.1f}% 숏스퀴즈 가능 (+8)")
        elif short_interest.short_pct > 50:
            score += 3
            factors.append(f"🟡 공매도 {short_interest.short_pct:.1f}% 높은 편 (+3)")

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

    if price and price.change_pct <= -5:
        score += 5
        factors.append(f"🟢 큰 폭 하락 → 매수 기회 (+5)")
    elif price and price.change_pct >= 5:
        score -= 3
        factors.append(f"🟡 큰 폭 상승 → 추격 주의 (-3)")

    return max(0, min(100, score)), factors


def _claude_dca_analysis(technicals, options, short_interest, insider_trades, news, rule_score, rule_factors):
    """BUG 4 FIX: 가격 숫자 제거, 결론(verdict) 명확화"""
    context = f"""당신은 $HOOD(Robinhood Markets) DCA 장기 투자자를 위한 시그널 분석가입니다.
주가 숫자는 알려주지 않습니다 (투자자 요청). 기술적/시장 지표만으로 분석해주세요.

현재 지표:
- RSI(14): {technicals.rsi_14} ({'과매도' if technicals.rsi_14 <= 30 else '과매수' if technicals.rsi_14 >= 70 else '중립'})
- MACD 히스토그램: {technicals.macd_histogram:.4f} (양수=상승모멘텀)
- MACD 시그널: {technicals.macd_alert or "없음"}
- 옵션 PCR: {options.pcr:.3f if options else "N/A"} ({options.pcr_signal if options else ""})
- 공매도 비율: {short_interest.short_pct:.1f}% if short_interest else "N/A"
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
# Slack 포맷터
# ─────────────────────────────────────────────
def format_technicals_block(ts: TechnicalSignals) -> dict:
    lines = []
    if ts.rsi_14 <= 30:
        lines.append(f"🟢 *과매도 구간* (RSI {ts.rsi_14}) — DCA 추가매수 타이밍 가능")
    elif ts.rsi_14 <= 40:
        lines.append(f"🟡 *약세 흐름* (RSI {ts.rsi_14}) — 매수 관심 구간")
    elif ts.rsi_14 >= 70:
        lines.append(f"🔴 *과열 구간* (RSI {ts.rsi_14}) — 추격 매수 자제")
    else:
        lines.append(f"⚪ *중립* (RSI {ts.rsi_14})")

    if ts.macd_alert == "bullish_cross":
        lines.append("🟢 *MACD 골든크로스* — 상승 전환 시그널")
    elif ts.macd_alert == "bearish_cross":
        lines.append("🔴 *MACD 데드크로스* — 하락 전환 시그널")

    return {"type": "section", "text": {"type": "mrkdwn", "text": "*📊 기술 지표*\n" + "\n".join(lines)}}


def format_options_block(od: OptionsData) -> dict:
    sig = {"heavy_hedging": "🟡 과도한 풋 헤징 (역발상 매수 시그널)", "bullish": "🟢 콜 우세 (낙관)", "neutral": "⚪ 중립"}
    return {"type": "section", "text": {"type": "mrkdwn", "text": (
        f"*📈 옵션 시장*\nPCR: *{od.pcr:.3f}* — {sig.get(od.pcr_signal, '')}\n"
        f"풋 OI: {od.total_puts:,} | 콜 OI: {od.total_calls:,}"
    )}}


def format_short_block(si: ShortInterestData) -> dict:
    emoji = "🔴" if si.signal == "high_short" else "⚪"
    return {"type": "section", "text": {"type": "mrkdwn", "text": (
        f"*🩳 공매도* ({si.date})\n{emoji} 비율: *{si.short_pct:.1f}%*"
    )}}


def format_insider_block(trades: list) -> list:
    if not trades:
        return []
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*🕴 내부자 거래*"}}]
    for t in trades[:5]:
        emoji = "🟢 매수" if t.trade_type == "Purchase" else "🔴 매도"
        scale = "대규모" if t.total_value >= 1_000_000 else "중규모" if t.total_value >= 100_000 else "소규모"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"{emoji} — *{t.filer}* ({t.title}) | {t.shares:,}주 {scale} | {t.date}"}})
    return blocks


def format_13f_block(filings: list) -> list:
    """BUG 3 FIX: 주식 수 / 평가금액 표시"""
    if not filings:
        return []
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*🏛 13F 기관 포지션*"}}]
    for f in filings[:8]:
        detail = ""
        if f.shares > 0:
            val_str = f"${f.value_usd/1_000_000:.1f}M" if f.value_usd >= 1_000_000 else f"${f.value_usd:,.0f}"
            detail = f" | {f.shares:,}주 / {val_str}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
            f"📋 *{f.institution}*{detail}\n제출일: {f.filing_date} | <{f.url}|SEC 보기>"}})
    return blocks


def format_news_block(news: list) -> list:
    """BUG 2 FIX: skip 된 뉴스 제외, 한국어만 표시"""
    relevant = [n for n in news if not n.get("skip") and n.get("summary")]
    if not relevant:
        return []
    lines = []
    for n in relevant[:5]:
        tag = "🟢 호재" if n.get("sentiment") == "positive" else "🔴 악재" if n.get("sentiment") == "negative" else "⚪ 중립"
        lines.append(f"• {tag} — {n['summary']}")
    return [{"type": "section", "text": {"type": "mrkdwn", "text": "*📰 뉴스 요약*\n" + "\n".join(lines)}}]


def format_dca_block(signal: DCASignal) -> dict:
    """BUG 4 FIX: verdict(결론) 맨 위에 강조"""
    bar_filled = signal.score // 5
    bar = ("🟩" if signal.score >= 60 else "🟨" if signal.score >= 40 else "🟥") * bar_filled + "⬜" * (20 - bar_filled)
    text = (
        f"*🎯 DCA 시그널: {signal.score}/100*\n"
        f"{bar}\n"
        f"*{signal.verdict}*\n"
        f"{signal.summary}"
    )
    if signal.factors:
        text += "\n" + "\n".join(f"  • {f}" for f in signal.factors[:4])
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


# ─────────────────────────────────────────────
# Slack 전송
# ─────────────────────────────────────────────
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
            state["price_alert_max_pct"] = abs_pct if direction != prev_dir else max(prev_max, abs_pct)
            state["price_alert_direction"] = direction
            ws.setdefault("alerts_fired", []).append(f"주가 {price.change_pct:+.1f}%")

    closes = fetch_price_history(60)
    technicals = TechnicalSignals()
    if closes:
        state["price_history"] = closes[-60:]
        technicals = get_technical_signals(closes)
        if technicals.rsi_alert or technicals.macd_alert:
            blocks.append(format_technicals_block(technicals))
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
        blocks.append({"type": "divider"})
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

    price = fetch_price()
    if price and price.prev_close > 0:
        abs_pct = abs(price.change_pct)
        direction = "up" if price.change_pct > 0 else "down"

        # ── BUG 1 FIX: 종가 기준 알림은 state에 저장해두고 morning에서 꺼냄 ──
        if abs_pct >= 4:
            state["pending_morning_alert"] = {
                "change_pct": round(price.change_pct, 1),
                "abs_pct": round(abs_pct, 1),
                "direction": direction,
                "date": datetime.now(KST).strftime("%Y-%m-%d"),
            }
            log.info(f"Morning alert queued: {price.change_pct:+.1f}%")
        else:
            state["pending_morning_alert"] = None

        # 장 마감 알림 (4%+ 시에만)
        if abs_pct >= 4:
            emoji = "🚀" if direction == "up" else "💥"
            label = "상승" if direction == "up" else "하락"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                f"{emoji} *$HOOD 종가 {abs_pct:.1f}% {label}* — 내일 08:00 KST 재알림 예정"}})

        # 알림 추적 리셋 (장 마감 = 하루 끝)
        state["price_alert_max_pct"] = 0
        state["price_alert_direction"] = ""

    closes = fetch_price_history(60)
    technicals = get_technical_signals(closes) if closes else TechnicalSignals()
    blocks.append(format_technicals_block(technicals))

    options = fetch_options_pcr()
    if options:
        blocks.append(format_options_block(options))
        ws.setdefault("pcr_readings", []).append(options.pcr)

    short = fetch_short_interest()
    if short:
        blocks.append(format_short_block(short))
        ws.setdefault("short_readings", []).append(short.short_pct)

    insider_trades = fetch_insider_trades()
    new_insiders = [t for t in insider_trades
                    if hashlib.md5(f"{t.filer}{t.date}{t.shares}".encode()).hexdigest()[:12]
                    not in state.get("last_insider_hashes", [])]
    if new_insiders:
        blocks.extend(format_insider_block(new_insiders))

    news = fetch_news()
    news = translate_news(news)

    dca = calculate_dca_signal(price or PriceData(), technicals, options, short, insider_trades, news)
    blocks.append(format_dca_block(dca))

    blocks.insert(0, {"type": "header", "text": {"type": "plain_text",
        "text": f"🔔 $HOOD 장 마감 — {datetime.now(KST).strftime('%m/%d')}"}})
    blocks.append({"type": "divider"})
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
        {"type": "divider"},
    ]
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
        blocks.append({"type": "divider"})
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

    blocks.append(format_technicals_block(technicals))

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
    blocks.append(format_dca_block(dca))
    blocks.append({"type": "divider"})

    send_slack(blocks)

    save_weekly_state({
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "alerts_fired": [], "insider_trades": [], "news_headlines": [],
        "rsi_readings": [], "pcr_readings": [], "short_readings": [],
    })
    log.info("Weekly done")


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
    }.get(mode, lambda: log.error(f"Unknown mode: {mode}"))()


if __name__ == "__main__":
    main()
