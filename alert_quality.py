from __future__ import annotations

from typing import Optional

from monitor_models import (
    DCATechnicalScore,
    OptionsData,
    PriceData,
    ShortInterestData,
    TechnicalSignals,
)
from slack_blocks import context_block, divider_block, section_block


ALERT_LEVELS = {
    "info": (0, "참고", "⚪"),
    "watch": (1, "주의", "🟡"),
    "urgent": (2, "긴급", "🔴"),
}


def alert_level(level: str) -> tuple:
    return ALERT_LEVELS.get(level, ALERT_LEVELS["info"])


def add_reason(reasons: list, level: str, text: str) -> None:
    if text:
        reasons.append((level, text))


def strongest_level(reasons: list) -> str:
    if not reasons:
        return "info"
    return max((level for level, _ in reasons), key=lambda item: alert_level(item)[0])


def mode_label(mode: str) -> str:
    return {
        "normal": "장중",
        "close": "장마감",
        "morning": "아침",
        "weekly": "주간",
        "13f": "13F",
        "dca_status": "DCA",
        "dca_update": "DCA",
    }.get(mode, mode)


def summarize_price_reason(price: Optional[PriceData], reasons: list) -> None:
    if not price or price.prev_close <= 0:
        return

    direction = "양전" if price.change_pct >= 0 else "음전"
    add_reason(reasons, "watch", f"오늘 방향: {direction}")

    if price.volume > 0 and price.vol_avg_5d > 0:
        vol_ratio = price.volume / price.vol_avg_5d
        if vol_ratio >= 1.5:
            add_reason(reasons, "watch", f"거래량이 5일 평균의 {vol_ratio:.1f}배: 수급 변화 확인 필요")


def summarize_technical_reason(technicals: Optional[TechnicalSignals], reasons: list) -> None:
    if not technicals:
        return

    if technicals.rsi_14 <= 30:
        add_reason(reasons, "watch", f"RSI {technicals.rsi_14:.1f}: 과매도 구간")
    elif technicals.rsi_14 >= 70:
        add_reason(reasons, "watch", f"RSI {technicals.rsi_14:.1f}: 과열 구간")

    if technicals.macd_alert == "bullish_cross":
        add_reason(reasons, "info", "MACD 골든크로스: 단기 모멘텀 개선")
    elif technicals.macd_alert == "bearish_cross":
        add_reason(reasons, "watch", "MACD 데드크로스: 단기 모멘텀 약화")


def summarize_flow_reason(options: Optional[OptionsData], short: Optional[ShortInterestData], reasons: list) -> None:
    if options:
        if options.pcr_signal == "heavy_hedging":
            add_reason(reasons, "watch", f"옵션 PCR {options.pcr:.2f}: 풋 헤징이 높음")
        elif options.pcr_signal == "bullish":
            add_reason(reasons, "info", f"옵션 PCR {options.pcr:.2f}: 콜 우세")

    if short:
        if short.short_pct >= 60:
            add_reason(reasons, "urgent", f"공매도 비중 {short.short_pct:.1f}%: 매우 높은 압박")
        elif short.short_pct >= 50:
            add_reason(reasons, "watch", f"공매도 비중 {short.short_pct:.1f}%: 높은 편")


def summarize_insider_reason(insiders: list, reasons: list) -> None:
    if not insiders:
        return

    sale_value = sum(t.total_value for t in insiders if t.trade_type == "Sale")
    purchase_value = sum(t.total_value for t in insiders if t.trade_type == "Purchase")
    sale_count = sum(1 for t in insiders if t.trade_type == "Sale")
    purchase_count = sum(1 for t in insiders if t.trade_type == "Purchase")

    if sale_count:
        level = "urgent" if sale_value >= 5_000_000 else "watch"
        scale = "대규모" if sale_value >= 1_000_000 else "중규모" if sale_value >= 100_000 else "소규모"
        add_reason(reasons, level, f"신규 내부자 매도 {sale_count}건, {scale}")
    if purchase_count:
        scale = "대규모" if purchase_value >= 1_000_000 else "중규모" if purchase_value >= 100_000 else "소규모"
        add_reason(reasons, "info", f"신규 내부자 매수 {purchase_count}건, {scale}")


def summarize_news_reason(news: list, reasons: list) -> None:
    relevant = [item for item in news if not item.get("skip") and item.get("summary")]
    if not relevant:
        return

    negative = sum(1 for item in relevant if item.get("sentiment") == "negative")
    positive = sum(1 for item in relevant if item.get("sentiment") == "positive")
    if negative:
        add_reason(reasons, "watch", f"관련 뉴스 {len(relevant)}건 중 부정 {negative}건")
    elif positive:
        add_reason(reasons, "info", f"관련 뉴스 {len(relevant)}건 중 긍정 {positive}건")
    else:
        add_reason(reasons, "info", f"관련 뉴스 {len(relevant)}건 업데이트")


def summarize_dca_reason(dca_tech: Optional[DCATechnicalScore], reasons: list) -> None:
    if not dca_tech:
        return

    if dca_tech.total >= 80:
        add_reason(reasons, "info", f"DCA 점수 {dca_tech.total}/100: {dca_tech.grade}")
    elif dca_tech.total <= 25:
        add_reason(reasons, "watch", f"DCA 점수 {dca_tech.total}/100: 진입 매력 낮음")


def build_alert_quality_blocks(
    display_ticker: str,
    mode: str,
    *,
    price: Optional[PriceData] = None,
    technicals: Optional[TechnicalSignals] = None,
    options: Optional[OptionsData] = None,
    short: Optional[ShortInterestData] = None,
    insiders: list | None = None,
    news: list | None = None,
    dca_tech: Optional[DCATechnicalScore] = None,
    extra_reasons: list | None = None,
) -> list:
    reasons = []
    summarize_price_reason(price, reasons)
    summarize_technical_reason(technicals, reasons)
    summarize_flow_reason(options, short, reasons)
    summarize_insider_reason(insiders or [], reasons)
    summarize_news_reason(news or [], reasons)
    summarize_dca_reason(dca_tech, reasons)
    for level, text in extra_reasons or []:
        add_reason(reasons, level, text)

    if not reasons:
        add_reason(reasons, "info", "새로운 핵심 시그널은 없고 정기 점검 결과만 공유")

    strongest = strongest_level(reasons)
    _, label, emoji = alert_level(strongest)

    top_reasons = sorted(reasons, key=lambda item: alert_level(item[0])[0], reverse=True)[:4]
    reason_lines = [
        f"• {alert_level(level)[2]} {text}"
        for level, text in top_reasons
    ]
    return [
        section_block(
            f"*{emoji} {label} | {display_ticker} {mode_label(mode)} 핵심 요약*\n"
            + "\n".join(reason_lines)
        ),
        context_block("분류 기준: 주가 급변, 기술지표, 옵션/공매도, SEC 내부자 거래, 뉴스, DCA 점수"),
        divider_block(),
    ]


def insert_alert_quality_summary(blocks: list, display_ticker: str, mode: str, **kwargs) -> None:
    summary = build_alert_quality_blocks(display_ticker, mode, **kwargs)
    insert_at = 1 if blocks and blocks[0].get("type") == "header" else 0
    if insert_at < len(blocks) and blocks[insert_at].get("type") == "divider":
        blocks.pop(insert_at)
    for block in reversed(summary):
        blocks.insert(insert_at, block)
