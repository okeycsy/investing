#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

import requests

import hood_monitor as hm
import market_scan as ms
from monitor_config import load_monitor_config


def _ctx(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _sec(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _divider() -> dict:
    return {"type": "divider"}


def _format_price(price) -> tuple[str, str]:
    if not price or price.prev_close <= 0:
        return "가격 데이터 없음", "info"
    direction = "상승" if price.change_pct >= 0 else "하락"
    level = "watch" if abs(price.change_pct) >= 2 else "info"
    line = (
        f"현재가 ${price.current:.2f}, 전일 대비 {price.change_pct:+.2f}% {direction} "
        f"({price.market_state or 'UNKNOWN'})"
    )
    if price.volume > 0:
        line += f", 거래량 {price.volume:,}"
    return line, level


def _format_technicals(technicals) -> tuple[str, str]:
    if not technicals:
        return "기술지표 데이터 없음", "info"
    signals = []
    level = "info"
    if technicals.rsi_14 <= 30:
        signals.append("RSI 과매도")
        level = "watch"
    elif technicals.rsi_14 >= 70:
        signals.append("RSI 과열")
        level = "watch"
    else:
        signals.append("RSI 중립권")
    if technicals.macd_alert:
        signals.append(f"MACD {technicals.macd_alert}")
        if technicals.macd_alert == "bearish_cross":
            level = "watch"
    return f"RSI {technicals.rsi_14:.1f}, " + ", ".join(signals), level


def _format_news(news: list) -> tuple[str, str]:
    relevant = hm.news_relevant_items(news)
    candidates = hm.news_candidate_items(news)
    if not relevant and candidates:
        titles = "; ".join((n.get("candidate_summary") or n.get("title", ""))[:45] for n in candidates[:3])
        level = "watch" if any(n.get("candidate_level") == "watch" for n in candidates) else "info"
        return f"관련 확정 0건, VRT 확인 후보 {len(candidates)}건: {titles}", level
    if not relevant:
        return f"뉴스 후보 {len(news)}건, VRT 키워드 후보 0건", "info"
    negative = sum(1 for n in relevant if n.get("sentiment") == "negative")
    level = "watch" if negative else "info"
    headlines = "; ".join(n.get("summary", "") for n in relevant[:3])
    if candidates:
        if any(n.get("candidate_level") == "watch" for n in candidates):
            level = "watch"
        return f"관련 뉴스 {len(relevant)}건: {headlines} | 확인 후보 {len(candidates)}건", level
    return f"관련 뉴스 {len(relevant)}건: {headlines}", level


def _format_insiders(trades: list) -> tuple[str, str]:
    if not trades:
        return "최근 Form 4 거래 없음", "info"
    sales = [t for t in trades if t.trade_type == "Sale"]
    purchases = [t for t in trades if t.trade_type == "Purchase"]
    awards = [t for t in trades if t.trade_type == "Award"]
    latest = max((t.date for t in trades if t.date), default="")
    if sales:
        value = sum(t.total_value for t in sales)
        text = f"최근 Form 4 {len(trades)}건, 장내 매도 {len(sales)}건"
        if value > 0:
            text += f" (${value/1_000_000:.1f}M)"
        return text + (f", 최신 {latest}" if latest else ""), "watch"
    if purchases:
        return f"최근 Form 4 {len(trades)}건, 장내 매수 {len(purchases)}건" + (f", 최신 {latest}" if latest else ""), "info"
    if awards:
        return f"최근 Form 4 {len(trades)}건 모두/대부분 RSU·보상성 Award, 장내 매수·매도 없음" + (f", 최신 {latest}" if latest else ""), "info"
    return f"최근 Form 4 {len(trades)}건 확인" + (f", 최신 {latest}" if latest else ""), "info"


def _market_scan_summary(ticker: str) -> tuple[str, str]:
    macro = ms.fetch_macro_context()
    ohlcv_map = ms.batch_download([ticker], period="6mo")
    ts = ms.score_ticker(
        ticker,
        ms.sector_for_ticker(ticker),
        ohlcv_map.get(ticker, {}),
        btc_above_sma20=macro["btc_above_sma20"],
        vix=macro["vix"],
    )
    if ts.error:
        return "단일 종목 스캔 데이터 없음", "watch"
    level = "watch" if ts.score < 40 else "info"
    if ts.score >= 80:
        level = "info"
    return (
        f"Market Scan {ts.score}점 ({ts.grade}) | RSI {ts.rsi:.1f}, CMF {ts.cmf:+.3f}, "
        f"EvsR {ts.evsr:.2f}, VIX {macro.get('vix', 0):.1f}",
        level,
    )


def _level_label(level: str) -> tuple[str, str]:
    if level == "urgent":
        return "긴급", "🔴"
    if level == "watch":
        return "주의", "🟡"
    return "참고", "⚪"


def build_digest() -> tuple[str, list]:
    config = load_monitor_config()
    ticker = config.ticker
    display = config.display_ticker
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    price = hm.fetch_price(realtime=False)
    closes = hm.fetch_price_history(60)
    technicals = hm.get_technical_signals(closes) if closes else hm.TechnicalSignals()
    news = hm.translate_news(hm.fetch_news())
    insiders = hm.fetch_insider_trades()

    rows = [
        ("가격", *_format_price(price)),
        ("기술지표", *_format_technicals(technicals)),
        ("뉴스", *_format_news(news)),
        ("SEC Form 4", *_format_insiders(insiders)),
        ("종목 스캔", *_market_scan_summary(ticker)),
    ]
    strongest = "watch" if any(level == "watch" for _, _, level in rows) else "info"
    label, emoji = _level_label(strongest)
    lines = [f"• {_level_label(level)[1]} *{name}*: {text}" for name, text, level in rows]

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"{display} 실제 데이터 시작 점검"}},
        _sec(f"*{emoji} {label} | {display} 현재 실제 데이터 요약*\n" + "\n".join(lines)),
        _ctx(f"{config.company_name or ticker} | {config.profile_context or 'profile context unavailable'} | 샘플 아님: Yahoo/SEC/뉴스/Market Scan 실제 조회 | {now}"),
        _divider(),
    ]
    news_blocks = hm.format_news_block(news)
    if news_blocks:
        blocks.extend(news_blocks)
        blocks.append(_divider())
    return f"{display} actual startup digest", blocks


def send_slack(text: str, blocks: list) -> tuple[bool, str]:
    webhook = os.environ.get("SLACK_WEBHOOK_URL") or os.environ.get("MARKET_SCAN_WEBHOOK")
    if not webhook:
        return False, "Slack webhook missing"
    response = requests.post(webhook, json={"text": text, "blocks": blocks}, timeout=15)
    if response.status_code != 200:
        return False, f"Slack HTTP {response.status_code}: {response.text[:120]}"
    return True, "sent"


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a real current-data startup digest.")
    parser.add_argument("--no-slack", action="store_true", help="Only print the digest text.")
    args = parser.parse_args()

    text, blocks = build_digest()
    for block in blocks:
        if block.get("type") == "section":
            print(block["text"]["text"])
        elif block.get("type") == "context":
            print(block["elements"][0]["text"])

    if args.no_slack:
        return 0

    ok, detail = send_slack(text, blocks)
    print(f"Slack: {detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
