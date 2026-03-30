"""
$HOOD Slack Monitor v3
- 장중 가격: ±4% 초과 시 즉시 알림, 이후 1%씩 새 구간마다 추가 알림
  (되돌아온 구간은 재알림 없음)
- 장 마감 후: 종가 기준 ±4% 이상이면 다음날 08:00 KST 알림
- 뉴스: 긍정/부정/SEC/애널리스트 분류 + Claude로 한국어 번역
"""

import os
import json
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from pathlib import Path

SLACK_WEBHOOK_URL  = os.environ["SLACK_WEBHOOK_URL"]
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
TICKER             = "HOOD"
PRICE_THRESHOLD    = 0.04   # ±4% 기준
NEWS_LOOKBACK_MIN  = 65
RUN_MODE           = os.environ.get("RUN_MODE", "normal")
# RUN_MODE:
#   normal   — 장중 뉴스 + 장중 가격 체크 (30분마다)
#   close    — 장 마감 후 종가 확정 + 다음날 아침 알림 예약
#   morning  — 08:00 KST 아침 종가 알림 전송

STATE_FILE = Path("price_state.json")


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
SEC_FORM_DESC = {
    "8-K":     "주요 사건 공시 (실적, 경영진 변경 등)",
    "10-K":    "연간 보고서",
    "10-Q":    "분기 보고서",
    "DEF 14A": "주주총회 위임장",
    "Form 4":  "내부자 지분 변동 신고",
    "S-1":     "신규 주식 등록 신청",
}


# ── Claude 번역 ────────────────────────────────────────────────
def translate_to_korean(text: str) -> str:
    if not ANTHROPIC_API_KEY or not text.strip():
        return text
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 300,
                "messages": [{
                    "role": "user",
                    "content": (
                        "다음 미국 주식 관련 영문 텍스트를 자연스러운 한국어 금융 표현으로 번역해줘. "
                        "번역문만 출력하고 다른 말은 하지 마.\n\n"
                        f"{text}"
                    )
                }]
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()
    except Exception as e:
        print(f"[번역 오류] {e}")
        return text


# ── Slack 전송 ────────────────────────────────────────────────
COLOR_MAP = {
    "positive":   "#22c55e",
    "negative":   "#ef4444",
    "price_up":   "#f59e0b",
    "price_down": "#ef4444",
    "sec":        "#3b82f6",
    "analyst":    "#8b5cf6",
    "neutral":    "#6b7280",
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


# ── 상태 저장/로드 ────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ── 가격 조회 ─────────────────────────────────────────────────
def fetch_price_data() -> dict:
    """당일 시가, 현재가, 전일 종가 반환"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}"
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"interval": "1d", "range": "5d"}
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    result = data["chart"]["result"][0]
    quotes = result["indicators"]["quote"][0]
    closes = [c for c in quotes["close"] if c is not None]
    meta   = result.get("meta", {})
    return {
        "prev_close":    closes[-2] if len(closes) >= 2 else None,
        "latest_close":  closes[-1] if closes else None,
        "current_price": meta.get("regularMarketPrice") or (closes[-1] if closes else None),
        "market_state":  meta.get("marketState", "CLOSED"),  # REGULAR, PRE, POST, CLOSED
    }


# ── 장중 가격 알림 (normal 모드) ──────────────────────────────
def check_intraday_price(state: dict) -> dict:
    """
    전일 종가 대비 현재가 변동률 계산.
    - 처음 ±4% 돌파 시 즉시 알림
    - 이후 max/min 대비 1% 새 구간 돌파마다 알림
    - 되돌아온 구간은 재알림 없음
    """
    try:
        pd = fetch_price_data()
        prev_close   = pd["prev_close"]
        current      = pd["current_price"]
        market_state = pd["market_state"]

        if not prev_close or not current:
            print("[장중가격] 데이터 없음")
            return state

        change_pct = (current - prev_close) / prev_close
        print(f"[장중가격] 현재 {change_pct:+.2%} (market={market_state})")

        # 장외 시간이면 스킵 (PRE/POST/CLOSED)
        if market_state != "REGULAR":
            print(f"[장중가격] 장외시간({market_state}), 스킵")
            return state

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # 날짜 바뀌면 장중 알림 구간 초기화
        if state.get("intraday_date") != today:
            state["intraday_date"]       = today
            state["intraday_alerted_up"]   = []   # 알림 발송한 상단 구간 목록 (예: [4,5,6])
            state["intraday_alerted_down"] = []   # 알림 발송한 하단 구간 목록 (예: [-4,-5])

        pct_floor = int(change_pct * 100)  # 소수점 버림 → 구간 인덱스

        if change_pct >= PRICE_THRESHOLD:
            # 상승 구간 체크
            alerted = state.get("intraday_alerted_up", [])
            # 현재 구간(pct_floor %)부터 4%까지 새 구간 찾기
            for level in range(pct_floor, 3, -1):  # 현재구간 → 4 순으로
                if level not in alerted:
                    alerted.append(level)
                    direction = "급등" if level == 4 else f"+{level}% 돌파"
                    send_slack(
                        text=f"전일 종가 대비 *+{level}%* 구간 돌파 중이에요.",
                        category="price_up",
                        title=f"${TICKER} 장중 {direction}",
                    )
                    print(f"[장중가격] +{level}% 알림 전송")
            state["intraday_alerted_up"] = alerted

        elif change_pct <= -PRICE_THRESHOLD:
            # 하락 구간 체크 (음수)
            alerted = state.get("intraday_alerted_down", [])
            for level in range(pct_floor, -3, 1):  # 현재구간 → -4 순으로 (음수)
                if level not in alerted and level <= -4:
                    alerted.append(level)
                    abs_level = abs(level)
                    direction = "급락" if abs_level == 4 else f"-{abs_level}% 돌파"
                    send_slack(
                        text=f"전일 종가 대비 *-{abs_level}%* 구간 돌파 중이에요.",
                        category="price_down",
                        title=f"${TICKER} 장중 {direction}",
                    )
                    print(f"[장중가격] -{abs_level}% 알림 전송")
            state["intraday_alerted_down"] = alerted

    except Exception as e:
        print(f"[장중가격 오류] {e}")

    return state


# ── 장 마감 종가 확정 (close 모드) ────────────────────────────
def check_close_price(state: dict) -> dict:
    """장 마감 후 종가 기준 ±4% 이상이면 다음날 아침 알림 예약"""
    try:
        pd = fetch_price_data()
        prev_close  = pd["prev_close"]
        final_close = pd["latest_close"]

        if not prev_close or not final_close:
            print("[종가] 데이터 없음")
            return state

        change_pct = (final_close - prev_close) / prev_close
        print(f"[종가] {change_pct:+.2%} (종가 ${final_close:.2f})")

        if abs(change_pct) >= PRICE_THRESHOLD:
            cat = "price_up" if change_pct > 0 else "price_down"
            state["morning_alert"] = {
                "category":   cat,
                "change_pct": f"{change_pct:+.1%}",
                "date":       datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            }
            print(f"[종가] 아침 알림 예약: {change_pct:+.1%}")
        else:
            state.pop("morning_alert", None)
            print("[종가] 기준 미달, 아침 알림 없음")

    except Exception as e:
        print(f"[종가 오류] {e}")

    return state


# ── 아침 알림 전송 (morning 모드) ─────────────────────────────
def send_morning_alert(state: dict) -> dict:
    alert = state.get("morning_alert")
    if not alert:
        print("[아침] 예약된 알림 없음")
        return state

    cat       = alert["category"]
    direction = "급등" if cat == "price_up" else "급락"
    send_slack(
        text=f"어제 미국 장 종가 기준 *{alert['change_pct']}* {direction}이 있었어요.",
        category=cat,
        title=f"${TICKER} 전일 종가 {direction}",
    )
    print(f"[아침] 알림 전송: {alert}")
    state.pop("morning_alert", None)
    return state


# ── 뉴스 분류 ─────────────────────────────────────────────────
def classify(title: str, summary: str) -> str:
    text = (title + " " + summary).lower()
    if any(k in text for k in SEC_KW):    return "sec"
    if any(k in text for k in ANALYST_KW): return "analyst"
    pos = sum(1 for k in POSITIVE_KW if k in text)
    neg = sum(1 for k in NEGATIVE_KW if k in text)
    if pos > neg: return "positive"
    if neg > pos: return "negative"
    return "neutral"


def is_recent(entry) -> bool:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            pub = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        else:
            return True
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=NEWS_LOOKBACK_MIN)
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
            if not is_recent(entry): continue
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", "")
            cat     = classify(title, summary)
            if cat == "neutral":
                print(f"[Yahoo/skip] {title[:60]}")
                continue
            print(f"[Yahoo/{cat}] {title[:60]}")
            trans_title   = translate_to_korean(title)
            trans_summary = translate_to_korean(summary) if summary else ""
            label_map = {"positive": "긍정 뉴스", "negative": "부정 뉴스",
                         "sec": "SEC 공시", "analyst": "애널리스트"}
            body = trans_title
            if trans_summary:
                body += f"\n_{trans_summary[:120]}_"
            body += f"\n<{link}|원문 보기>"
            send_slack(text=body, category=cat, title=f"${TICKER} {label_map.get(cat, '뉴스')}")
            count += 1
        print(f"Yahoo 뉴스: {count}건 전송")
    except Exception as e:
        print(f"[Yahoo 뉴스 오류] {e}")


# ── SEC EDGAR 공시 ────────────────────────────────────────────
ROBINHOOD_CIK = "0001783398"

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
            if not is_recent(entry): continue
            title   = entry.get("title", "")
            link    = entry.get("link", "")
            summary = entry.get("summary", "")
            form_type = ""
            for kw in ["8-K", "10-K", "10-Q", "DEF 14A", "Form 4", "S-1"]:
                if kw.lower() in title.lower() or kw in summary:
                    form_type = kw
                    break
            if not form_type: continue
            print(f"[SEC/{form_type}] {title[:60]}")
            form_desc     = SEC_FORM_DESC.get(form_type, "")
            trans_title   = translate_to_korean(title)
            body = (
                f"*양식:* {form_type}"
                + (f" — {form_desc}" if form_desc else "")
                + f"\n{trans_title}"
                + f"\n<{link}|SEC EDGAR에서 보기>"
            )
            send_slack(text=body, category="sec", title=f"${TICKER} SEC 공시 — {form_type}")
            count += 1
        print(f"SEC 공시: {count}건 전송")
    except Exception as e:
        print(f"[SEC 오류] {e}")


# ── 메인 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    kst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M")
    print(f"=== $HOOD 모니터 시작 KST {kst} (mode={RUN_MODE}) ===")

    state = load_state()

    if RUN_MODE == "morning":
        state = send_morning_alert(state)

    elif RUN_MODE == "close":
        state = check_close_price(state)

    else:  # normal
        state = check_intraday_price(state)
        check_yahoo_news()
        check_sec_filings()

    save_state(state)
    print("=== 완료 ===")
