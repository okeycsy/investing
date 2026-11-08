"""Slack rendering for insight lines and the quarterly earnings-quality alert."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from investing_monitor.domain.insights import EarningsQuality, InsightLine, QUALITY_LABELS
from investing_monitor.presentation.timing import timestamp

CLOSE_INSIGHT_HEADING = "🔍 *오늘 평소와 다른 점*"
WEEKLY_INSIGHT_HEADING = "🔍 *주간 인사이트*"
QUALITY_HEADING = "이익의 질 체크포인트"


def close_insight_text(lines: Sequence[InsightLine], *, limit: int = 6) -> str:
    if not lines:
        return ""
    body = "\n".join(f"• {line.text}" for line in lines[:limit])
    return f"{CLOSE_INSIGHT_HEADING}\n{body}"


def weekly_insight_text(lines: Sequence[str], *, limit: int = 6) -> str:
    if not lines:
        return ""
    body = "\n".join(f"• {line}" for line in lines[:limit])
    return f"{WEEKLY_INSIGHT_HEADING}\n{body}"


def build_earnings_quality_message(
    ticker: str,
    quality: EarningsQuality,
    *,
    created_at: datetime,
    filing_url: str,
    baseline_note: str = "",
) -> dict:
    latest, prior = quality.quarters[-1], quality.quarters[-2]
    rows = []
    for key in (
        "revenue", "net_income", "operating_cash_flow", "capex", "receivables",
        "contract_assets", "inventory", "payables", "deferred_revenue",
        "customer_deposits", "cash", "long_term_debt",
    ):
        value = latest.values.get(key)
        if value is None:
            continue
        delta = ""
        if prior.values.get(key) is not None:
            delta = f" ({value - prior.values[key]:+,.0f} 전분기 대비)"
        rows.append(f"• {QUALITY_LABELS[key]} ${value:,.0f}M{delta}")
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🧪 ${ticker} {QUALITY_HEADING} · {latest.period_end:%Y-%m-%d}",
            },
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"출처 SEC XBRL ({quality.source_form}) · 분기 수치, $M · "
                    f"작성 {timestamp(created_at)}"
                ),
            }],
        },
        _section("*확인된 사실*\n" + "\n".join(rows)),
        _section("*판정*\n" + "\n".join(f"• {line}" for line in quality.verdict)),
    ]
    if baseline_note:
        blocks.append(_section(baseline_note))
    blocks.append(
        _section(
            "계약자산 = 매출로 인식했지만 아직 청구하지 않은 금액. 급증하면 이익과 현금흐름이 벌어진다.\n"
            f"<{filing_url}|SEC 원문 보기>"
        )
    )
    return {
        "text": f"${ticker} {latest.period_end:%Y-%m-%d} {QUALITY_HEADING}",
        "blocks": blocks,
    }


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}
