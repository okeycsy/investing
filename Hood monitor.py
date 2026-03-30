"""
$HOOD Slack Monitor
- 가격 ±5% 급등락 알림
- 뉴스 긍정/부정 분류 + SEC 공시 + 애널리스트 목표주가 알림
- 무료 소스 사용 (Yahoo Finance RSS, SEC EDGAR)
"""

import os
import json
import requests
import feedparser
from datetime import datetime, timezone, timedelta

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
TICKER = "HOOD"
PRICE_THRESHOLD = 0.05      # ±5%
NEWS_LOOKBACK_MINUTES = 65  # 실행 주기보다 약간 넉넉하게


# ── 키워드 사전 ──────────────────────────────────────────────
POSITIVE_KW = [
    "upgrade", "upgraded", "outperform", "buy", "strong buy",
    "overweight", "beat", "beats", "raise", "raised", "top",
    "record", "growth", "surge", "surges", "rally", "bullish",
    "profit", "expand", "partnership", "launch", "launches", "positive",
    "higher", "increases", "strong results", "revenue beat",
]
NEGATIVE_KW = [
    "downgrade", "downgraded", "underperform", "sell", "underweight",
    "miss", "misses", "cut", "cuts", "lower", "decline", "declines",
    "bearish", "loss", "lawsuit", "investigation", "fine", "penalty",
    "warning", "drops", "falls", "concern", "risks", "fraud",
    "weak results", "revenue miss",
]
SEC_KW = [
    "8-k", "10-k", "10-q", "sec filing", "earnings report",
    "quarterly results", "annual report", "form 4", "proxy",
]
ANALYST_KW = [
    "price target", "pt raised", "pt cut", "pt lowered", "pt increased",
    "initiates", "reiterates", "coverage", "analyst", "rating change",
    "sets price", "target price",
]


# ── Slack 전송 ────────────────────────────────────────────────
COLOR_MAP = {
    "positive": "#22c55e",
    "negative": "#ef4444",
    "price_up":  "#f59e0b",
    "price_down":"#ef4444",
    "sec":       "#3b82f6",
    "analyst":   "#8b5cf6",
    "neutral":   "#6b7280",
}

def send_slack(text: str, category: str, title: str = ""):
    color = COLOR_MAP.get(category, "#6b7280")
    emoji_map = {
        "positive": "🟢", "negative": "🔴",
        "price_up": "📈",  "price_down": "📉",
        "sec": "📋",       "analyst": "🎯",
        "neutral": "📰",
    }
    emoji = emoji_map.get(category, "📰")
    header = f"{emoji} *{title}*" if title else f"{emoji} *${TICKER} 알림*"

    payload = {
        "attachments": [{
            "color": color,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": header}},
                {"type": "section", "text": {"type": "mrkdwn", "text": text}},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": f"_KST {datetime.now(timezone(timedelta(hours=9))).strftime('%m/%d %H:%M')}_"}]},
            ]
        }]
    }
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()


# ── 가격 모니터링 ─────────────────────────────────────────────
def check_price():
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}"
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"interval": "1d", "range": "5d"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()

        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]

        if len(closes) < 2:
            print("가격 데이터 부족")
            return

        prev_close = closes[-2]
        curr_close = closes[-1]
        change_pct = (curr_close - prev_close) / prev_close

        print(f"가격: ${curr_close:.2f} ({change_pct:+.1%}) vs 전일 ${prev_close:.2f}")

        if abs(change_pct) >= PRICE_THRESHOLD:
            direction = "급등" if change_pct > 0 else "급락"
            cat = "price_up" if change_pct > 0 else "price_down"
            send_slack(
                text=(
                    f"현재가: *${curr_close:.2f}*\n"
                    f"전일 종가: ${prev_close:.2f}\n"
                    f"변동: *{change_pct:+.2%}*"
                ),
                category=cat,
                title=f"${TICKER} {direction} {change_pct:+.1%}",
            )

    except Exception as e:
        print(f"[가격 오류] {e}")


# ── 뉴스 분류 ─────────────────────────────────────────────────
def classify(title: str, summary: str) -> str:
    text = (title + " " + summary).lower()
    if any(k in text for k in SEC_KW):
        return "sec"
    if any(k in text for k in ANALYST_KW):
        return "analyst"
    pos = sum(1 for k in POSITIVE_KW if k in text)
    neg = sum(1 for k in NEGATIVE_KW if k in text)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def is_recent(entry) -> bool:
    """뉴스가 최근 NEWS_LOOKBACK_MINUTES 이내인지 확인"""
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            pub = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        else:
            return True  # 날짜 없으면 일단 처리
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=NEWS_LOOKBACK_MINUTES)
        return pub >= cutoff
    except Exception:
        return True


# ── Yahoo Finance 뉴스 ────────────────────────────────────────
def check_yahoo_news():
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={TICKER}&region=US&lang=en-US"
    try:
        feed = feedparser.parse(url)
        count = 0
        for entry in feed.entries:
            if not is_recent(entry):
                continue
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", "")
            cat     = classify(title, summary)

            print(f"[Yahoo/{cat}] {title[:60]}")
            label_map = {
                "positive": "긍정 뉴스",
                "negative": "부정 뉴스",
                "sec":      "SEC 공시",
                "analyst":  "애널리스트",
                "neutral":  "일반 뉴스",
            }
            label = label_map.get(cat, "뉴스")
            send_slack(
                text=f"{title}\n<{link}|기사 보기>",
                category=cat,
                title=f"${TICKER} {label}",
            )
            count += 1

        print(f"Yahoo 뉴스: {count}건 처리")
    except Exception as e:
        print(f"[Yahoo 뉴스 오류] {e}")


# ── SEC EDGAR 공시 ────────────────────────────────────────────
ROBINHOOD_CIK = "0001783398"   # Robinhood Markets, Inc.

def check_sec_filings():
    url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={ROBINHOOD_CIK}"
        f"&type=&dateb=&owner=include&count=5&search_text=&output=atom"
    )
    headers = {"User-Agent": "hood-monitor contact@example.com"}
    try:
        feed = feedparser.parse(url)
        count = 0
        for entry in feed.entries:
            if not is_recent(entry):
                continue
            title   = entry.get("title", "")
            link    = entry.get("link", "")
            summary = entry.get("summary", "")

            # 이미 Yahoo 뉴스에서 잡힐 수 있는 것 제외하고 SEC 원문만
            form_type = ""
            for kw in ["8-K", "10-K", "10-Q", "DEF 14A", "Form 4", "S-1"]:
                if kw.lower() in title.lower() or kw in summary:
                    form_type = kw
                    break

            if not form_type:
                continue  # 관련 없는 항목 스킵

            print(f"[SEC/{form_type}] {title[:60]}")
            send_slack(
                text=(
                    f"양식: *{form_type}*\n"
                    f"{title}\n"
                    f"<{link}|SEC EDGAR에서 보기>"
                ),
                category="sec",
                title=f"${TICKER} SEC 공시 — {form_type}",
            )
            count += 1

        print(f"SEC 공시: {count}건 처리")
    except Exception as e:
        print(f"[SEC 오류] {e}")


# ── 메인 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"=== $HOOD 모니터 시작 {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    check_price()
    check_yahoo_news()
    check_sec_filings()
    print("=== 완료 ===")
