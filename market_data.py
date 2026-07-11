from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
YAHOO_QUOTE_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
]

_last_yahoo_call = 0.0


class SyntheticYahooResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self._payload


def yahoo_throttle(min_interval: float = 1.5) -> None:
    global _last_yahoo_call
    elapsed = time.time() - _last_yahoo_call
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
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
        except Exception as exc:
            log.error(f"Request failed: {exc}")
            if attempt < retries - 1:
                time.sleep(1)
    return None


def fetch_yahoo_chart(ticker: str, params: dict, timeout: int = 15):
    """Try both Yahoo chart hosts because query1/query2 can be rate-limited independently."""
    for template in YAHOO_QUOTE_URLS:
        resp = safe_get(template.format(ticker=ticker), params=params, timeout=timeout)
        if resp:
            return resp
    fallback = fetch_yfinance_chart(ticker, params)
    if fallback:
        log.info(f"Yahoo chart fallback via yfinance: {ticker} {params}")
        return SyntheticYahooResponse(fallback)
    return None


def fetch_yfinance_chart(ticker: str, params: dict) -> Optional[dict]:
    try:
        import yfinance as yf
    except Exception as exc:
        log.warning(f"yfinance fallback unavailable: {exc}")
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
    except Exception as exc:
        log.warning(f"yfinance fallback failed ({ticker}): {exc}")
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
            timestamps.append(int(datetime.combine(idx.date(), datetime.min.time(), timezone.utc).timestamp()))

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
