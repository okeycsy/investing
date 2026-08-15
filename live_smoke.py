#!/usr/bin/env python3
"""
Live smoke checks for the configured ticker.

This intentionally performs real external calls:
- Yahoo Finance chart API
- SEC submissions and Form 4 XML
- Slack Incoming Webhook, when SLACK_WEBHOOK_URL or MARKET_SCAN_WEBHOOK is set
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

import requests

from monitor_config import load_monitor_config


YAHOO_CHART_URLS = [
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
]
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


def _status(ok: bool) -> str:
    return "OK" if ok else "FAIL"


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}


def _request_json(url: str, *, headers=None, params=None, timeout=20):
    response = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _request_yahoo_json(config, params: dict):
    last_error = ""
    for template in YAHOO_CHART_URLS:
        url = template.format(ticker=config.ticker)
        for attempt in range(3):
            response = requests.get(url, headers=DEFAULT_HEADERS, params=params, timeout=20)
            if response.status_code == 200:
                return response.json()
            last_error = f"{url} HTTP {response.status_code}: {response.text[:80].strip()}"
            if response.status_code in (429, 503):
                time.sleep(2 ** attempt)
                continue
            break
    fallback = _request_yahoo_via_yfinance(config, params)
    if fallback:
        return fallback
    raise RuntimeError(last_error or "Yahoo request failed")


def _request_yahoo_via_yfinance(config, params: dict):
    try:
        import yfinance as yf
    except Exception:
        return None

    try:
        df = yf.download(
            config.ticker,
            period=params.get("range", "10d"),
            interval=params.get("interval", "1d"),
            auto_adjust=False,
            progress=False,
            prepost=bool(params.get("includePrePost")),
            threads=False,
        )
    except Exception:
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

    closes = [float(v) for v in df["Close"].dropna().tolist()] if "Close" in df else []
    if not closes:
        return None
    return {
        "chart": {
            "result": [{
                "meta": {
                    "currency": "USD",
                    "regularMarketPrice": closes[-1],
                    "source": "yfinance-fallback",
                },
                "indicators": {"quote": [{"close": closes}]},
            }],
            "error": None,
        }
    }


def check_yahoo(config) -> tuple[bool, str]:
    data = _request_yahoo_json(config, {"interval": "1d", "range": "10d"})
    result = (data.get("chart", {}).get("result") or [None])[0]
    if not result:
        return False, "Yahoo chart returned no result"

    meta = result.get("meta", {})
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    closes = [c for c in quote.get("close", []) if c is not None]
    price = meta.get("regularMarketPrice") or (closes[-1] if closes else None)
    currency = meta.get("currency", "")
    source = meta.get("source", "chart-api")
    if price is None:
        return False, "Yahoo chart returned no price"
    return True, f"{config.display_ticker} price={float(price):.2f} {currency} via {source}".strip()


def check_sec(config) -> tuple[bool, str]:
    if not config.cik:
        return False, "CIK is not configured"

    cik = config.cik.strip().zfill(10)
    data = _request_json(SEC_SUBMISSIONS_URL.format(cik=cik), headers=config.sec_headers)

    name = data.get("name") or "Unknown"
    filings = data.get("filings", {}).get("recent", {}).get("accessionNumber", [])
    return True, f"SEC company={name}, recent_filings={len(filings)}"


def check_sec_form4(config) -> tuple[bool, str]:
    if not config.cik:
        return False, "CIK is not configured"

    cik = config.cik.strip().zfill(10)
    data = _request_json(SEC_SUBMISSIONS_URL.format(cik=cik), headers=config.sec_headers)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])

    for idx, form in enumerate(forms):
        if str(form).upper() not in {"4", "4/A"}:
            continue
        def recent_item(key: str) -> str:
            values = recent.get(key, [])
            if idx >= len(values):
                return ""
            return str(values[idx] or "")

        accession = recent_item("accessionNumber")
        primary_doc = recent_item("primaryDocument")
        if not accession or not primary_doc:
            continue

        acc_clean = accession.replace("-", "")
        filing_cik = accession.split("-", 1)[0].lstrip("0")
        primary_name = primary_doc.rsplit("/", 1)[-1]
        ciks = [filing_cik]
        issuer_cik = config.cik.strip().lstrip("0")
        if issuer_cik and issuer_cik not in ciks:
            ciks.append(issuer_cik)

        for cik_candidate in ciks:
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_candidate}/{acc_clean}/{primary_name}"
            response = requests.get(xml_url, headers=config.sec_legacy_headers, timeout=20)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            if "<ownershipDocument" in response.text:
                filing_date = recent_item("filingDate")
                return True, f"SEC Form 4 XML reachable ({filing_date or accession})"
            return False, "SEC Form 4 URL reached but XML ownership document was not found"

        return False, "SEC Form 4 XML was listed but archive paths returned 404"

    return True, "SEC Form 4 submissions reachable, no recent Form 4 entries"


def check_slack(config, *, require: bool) -> tuple[bool, str]:
    webhook = os.environ.get("SLACK_WEBHOOK_URL") or os.environ.get("MARKET_SCAN_WEBHOOK")
    if not webhook:
        msg = "Slack webhook missing; set SLACK_WEBHOOK_URL or MARKET_SCAN_WEBHOOK"
        return (False, msg) if require else (True, f"SKIP - {msg}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    payload = {
        "text": f"{config.display_ticker} live smoke test",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":white_check_mark: *{config.display_ticker} live smoke test*\nYahoo/SEC checks reached from GitHub/Codex runtime.\n_{now}_",
                },
            }
        ],
    }
    response = requests.post(webhook, json=payload, timeout=15)
    if response.status_code != 200:
        return False, f"Slack HTTP {response.status_code}: {response.text[:120]}"
    return True, "Slack message sent"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live Yahoo/SEC/Slack smoke checks.")
    parser.add_argument("--require-slack", action="store_true", help="Fail when no Slack webhook is configured.")
    parser.add_argument("--no-slack", action="store_true", help="Skip Slack even when a webhook is configured.")
    args = parser.parse_args()

    config = load_monitor_config()
    checks = [
        ("Yahoo", lambda: check_yahoo(config)),
        ("SEC submissions", lambda: check_sec(config)),
        ("SEC Form 4 XML", lambda: check_sec_form4(config)),
    ]
    if not args.no_slack:
        checks.append(("Slack", lambda: check_slack(config, require=args.require_slack)))

    failures = []
    print(f"Live smoke checks for {config.display_ticker} ({config.company_name or 'Unknown company'})")
    for name, fn in checks:
        try:
            ok, detail = fn()
        except Exception as exc:
            ok, detail = False, f"{exc.__class__.__name__}: {exc}"
        print(f"[{_status(ok)}] {name}: {detail}")
        if not ok:
            failures.append(name)

    if failures:
        print("Failed checks: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
