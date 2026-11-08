"""
Thin, fault-tolerant wrappers around OpenBB Platform (yfinance / sec / finra providers).

Every function returns a pandas DataFrame (possibly empty) or None and never raises,
so callers (a Slack bot running unattended) can degrade gracefully.
"""
from __future__ import annotations

import logging
import re
import html as _html
from datetime import date, timedelta
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests

log = logging.getLogger("obb_tools.data")

SEC_UA = {"User-Agent": "InvestingResearchBot/1.0 (seokyooong.research@gmail.com)", "Accept-Encoding": "gzip, deflate"}


@lru_cache(maxsize=1)
def obb():
    """Lazy import — the first import builds OpenBB's static assets (~15s)."""
    import warnings

    warnings.filterwarnings("ignore")
    from openbb import obb as _obb  # noqa: WPS433

    return _obb


def _df(call, *a, **kw) -> Optional[pd.DataFrame]:
    try:
        res = call(*a, **kw)
        df = res.to_df() if hasattr(res, "to_df") else pd.DataFrame(res)
        return df
    except Exception as e:  # noqa: BLE001
        log.warning("%s failed: %s", getattr(call, "__name__", call), str(e)[:200])
        return None


def _row(df: Optional[pd.DataFrame]) -> dict:
    if df is None or df.empty:
        return {}
    return df.iloc[0].to_dict()


# ───────────────────────── yfinance: price ─────────────────────────
def quote(sym: str) -> dict:
    return _row(_df(obb().yfinance.equity.price.quote, symbol=sym))


def history(sym: str, days: int = 400) -> Optional[pd.DataFrame]:
    start = (date.today() - timedelta(days=days)).isoformat()
    df = _df(obb().yfinance.equity.price.historical, symbol=sym, start_date=start)
    if df is not None and not df.empty:
        df.index = pd.to_datetime(df.index)
    return df


def histories(symbols: list[str], days: int = 400) -> pd.DataFrame:
    """Close prices for several symbols, aligned on date."""
    out = {}
    for s in symbols:
        h = history(s, days)
        if h is not None and not h.empty:
            out[s] = h["close"]
    return pd.DataFrame(out).dropna(how="all")


# ───────────────────────── yfinance: fundamentals ─────────────────────────
def profile(sym: str) -> dict:
    return _row(_df(obb().yfinance.equity.profile, symbol=sym))


def metrics(sym: str) -> dict:
    return _row(_df(obb().yfinance.equity.fundamental.metrics, symbol=sym))


def share_stats(sym: str) -> dict:
    return _row(_df(obb().yfinance.equity.ownership.share_statistics, symbol=sym))


def income(sym: str, period: str = "annual", limit: int = 5) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.equity.fundamental.income, symbol=sym, period=period, limit=limit)


def balance(sym: str, period: str = "quarter", limit: int = 5) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.equity.fundamental.balance, symbol=sym, period=period, limit=limit)


def cash(sym: str, period: str = "quarter", limit: int = 5) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.equity.fundamental.cash, symbol=sym, period=period, limit=limit)


# ───────────────────────── yfinance: estimates ─────────────────────────
def consensus(sym: str) -> dict:
    return _row(_df(obb().yfinance.equity.estimates.consensus, symbol=sym))


def recommendations(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.estimates.recommendations, symbol=sym)


def eps_trend(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.estimates.eps_trend, symbol=sym)


def eps_revisions(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.estimates.eps_revisions, symbol=sym)


def revenue_estimates(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.estimates.revenue, symbol=sym)


def earnings_estimates(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.estimates.earnings, symbol=sym)


def historical_eps(sym: str) -> Optional[pd.DataFrame]:
    df = _df(obb().yfinance.equity.fundamental.historical_eps, symbol=sym)
    if df is not None and not df.empty:
        df.index = pd.to_datetime(df.index)
    return df


def next_earnings_date(sym: str) -> Optional[date]:
    """Next scheduled earnings date: first historical_eps row without an actual, else calendar."""
    he = historical_eps(sym)
    if he is not None and not he.empty and "eps_actual" in he.columns:
        fut = he[he["eps_actual"].isna()]
        fut = fut[fut.index.date >= date.today()]
        if not fut.empty:
            return fut.index.min().date()
    cal = _row(_df(obb().yfinance.company_calendar, symbol=sym))
    ed = cal.get("earnings_date")
    if ed:
        try:
            d = pd.to_datetime(ed).date()
            if d >= date.today():
                return d
        except Exception:  # noqa: BLE001
            pass
    return None


# ───────────────────────── yfinance: ownership / news ─────────────────────────
def insider_trading_yf(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.equity.ownership.insider_trading, symbol=sym)


def institutional(sym: str) -> Optional[pd.DataFrame]:
    return _df(obb().yfinance.ownership.institutional, symbol=sym)


def news(sym: str) -> Optional[pd.DataFrame]:
    df = _df(obb().yfinance.news, symbol=sym)
    if df is not None and not df.empty:
        # normalise: index may be the date
        if "date" not in df.columns:
            df = df.reset_index().rename(columns={"index": "date"})
        if "url" not in df.columns and "link" in df.columns:
            df = df.rename(columns={"link": "url"})
    return df


# ───────────────────────── yfinance: options ─────────────────────────
def option_chains(sym: str) -> Optional[pd.DataFrame]:
    df = _df(obb().yfinance.derivatives.options.chains, symbol=sym)
    if df is not None and not df.empty:
        df["expiration"] = pd.to_datetime(df["expiration"])
    return df


# ───────────────────────── FINRA ─────────────────────────
def short_interest(sym: str) -> Optional[pd.DataFrame]:
    df = _df(obb().finra.equity.shorts.short_interest, symbol=sym)
    if df is not None and not df.empty:
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        df = df.sort_values("settlement_date")
    return df


# ───────────────────────── SEC ─────────────────────────
def company_filings(sym: str) -> Optional[pd.DataFrame]:
    df = _df(obb().sec.company_filings, symbol=sym)
    if df is not None and not df.empty:
        df["filing_date"] = pd.to_datetime(df["filing_date"])
        df = df.sort_values("filing_date", ascending=False)
    return df


def insider_trading_sec(sym: str, limit: int = 50) -> Optional[pd.DataFrame]:
    return _df(obb().sec.insider_trading, symbol=sym, limit=limit)


def cik(sym: str) -> Optional[str]:
    try:
        res = obb().sec.cik_map(symbol=sym)
        c = res.results.cik if hasattr(res.results, "cik") else res.to_df().iloc[0]["cik"]
        return str(int(c)).zfill(10)
    except Exception as e:  # noqa: BLE001
        log.warning("cik_map failed: %s", e)
        return None


def htm_text(url: str) -> str:
    """Download an SEC .htm and return readable text (tables flattened with ' | ')."""
    try:
        c = obb().sec.htm_file(url=url).results.content
    except Exception as e:  # noqa: BLE001
        log.warning("htm_file failed: %s", e)
        return ""
    return html_to_text(c)


def html_to_text(c: str) -> str:
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", c, flags=re.S | re.I)
    t = re.sub(r"<ix:header>.*?</ix:header>", "", t, flags=re.S | re.I)
    t = re.sub(r"</(p|div|tr|li|h\d)>", "\n", t, flags=re.I)
    t = re.sub(r"</t[dh]>", " | ", t, flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    t = _html.unescape(t).replace("\xa0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"(\s*\|\s*)+\n", "\n", t)
    t = re.sub(r"\n\s*\n+", "\n", t)
    return t


def filing_index_docs(index_url: str) -> list[str]:
    """List .htm documents inside a filing folder (uses the -index.htm page)."""
    try:
        r = requests.get(index_url, headers=SEC_UA, timeout=30)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        log.warning("filing index failed: %s", e)
        return []
    base = index_url.rsplit("/", 1)[0]
    rel = base.replace("https://www.sec.gov", "")
    links = re.findall(r'href="(' + re.escape(rel) + r'/[^"]+\.htm)"', r.text)
    return sorted({"https://www.sec.gov" + l for l in links})


def company_facts(cik10: str) -> dict:
    """Raw SEC XBRL companyfacts JSON (us-gaap facts)."""
    try:
        r = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json", headers=SEC_UA, timeout=60
        )
        r.raise_for_status()
        facts = r.json().get("facts", {})
        merged = dict(facts.get("us-gaap", {}))
        for k, v in facts.get("dei", {}).items():  # EntityCommonStockSharesOutstanding etc.
            merged.setdefault(k, v)
        return merged
    except Exception as e:  # noqa: BLE001
        log.warning("companyfacts failed: %s", e)
        return {}
