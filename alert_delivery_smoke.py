#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

import requests

from monitor_config import load_monitor_config


def _ctx(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _sec(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _message(title: str, body: str, context: str) -> list:
    return [
        {"type": "header", "text": {"type": "plain_text", "text": title}},
        _sec(body),
        _ctx(context),
        {"type": "divider"},
    ]


def build_smoke_messages(config) -> list[tuple[str, list]]:
    ticker = config.display_ticker
    company = config.company_name or "Unknown company"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ctx = f"샘플 알림 발송 점검 | 실제 데이터 아님 | {company} | {now}"

    return [
        (
            f"{ticker} 장중 알림 smoke",
            _message(
                f"{ticker} 장중 알림 점검",
                (
                    f"*🟡 주의 | {ticker} 장중 핵심 요약 샘플*\n"
                    "• 🟡 주가/거래량 급변 시 즉시 알림\n"
                    "• 🟡 RSI/MACD 신호 변화 표시\n"
                    "• ⚪ 신규 뉴스와 SEC 내부자 거래를 함께 요약"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 장마감 알림 smoke",
            _message(
                f"{ticker} 장마감 알림 점검",
                (
                    f"*🔴 긴급 | {ticker} 장마감 핵심 요약 샘플*\n"
                    "• 🔴 종가 급등락 기준 충족 시 최상단에 표시\n"
                    "• 🟡 옵션 PCR, 공매도, BTC 상관계수, 앱 순위 확인\n"
                    "• ⚪ DCA 기술지표 점수와 안전마진을 함께 표시"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 아침 브리핑 smoke",
            _message(
                f"{ticker} 아침 브리핑 점검",
                (
                    f"*🟡 주의 | {ticker} 아침 재확인 샘플*\n"
                    "• 🟡 전일 종가 급변동이 있었을 때 재알림\n"
                    "• ⚪ 장 시작 전 확인할 핵심 이유를 짧게 재정리"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 주간 브리핑 smoke",
            _message(
                f"{ticker} 주간 브리핑 점검",
                (
                    f"*⚪ 참고 | {ticker} 주간 요약 샘플*\n"
                    "• ⚪ 주간 가격 흐름, RSI, PCR, 공매도 평균 확인\n"
                    "• 🟡 주간 내부자 거래와 주요 뉴스가 있으면 상단 요약"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 13F 알림 smoke",
            _message(
                f"{ticker} 13F 알림 점검",
                (
                    f"*⚪ 참고 | {ticker} 기관 포지션 샘플*\n"
                    "• ⚪ 신규 13F 편입/보유 정보 감지 시 발송\n"
                    "• ⚪ 기관명, 주식 수, 평가금액을 요약"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} DCA 알림 smoke",
            _message(
                f"{ticker} DCA 알림 점검",
                (
                    f"*⚪ 참고 | {ticker} DCA 샘플*\n"
                    "• ⚪ DCA 현황 조회와 추가매수 등록 알림\n"
                    "• ⚪ 총 보유 수량, 평단, 매수 이력을 표시"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 단일 종목 스캔 smoke",
            _message(
                f"{ticker} 단일 종목 스캔 점검",
                (
                    f"*⚪ 참고 | {ticker} Market Scan 샘플*\n"
                    "• ⚪ 4-Layer 점수, CMF, EvsR, RSI, 매크로 상태 표시\n"
                    "• ⚪ 스케줄에서는 VRT 단일 종목만 스캔"
                ),
                ctx,
            ),
        ),
        (
            f"{ticker} 백테스트 리포트 smoke",
            _message(
                f"{ticker} 백테스트 리포트 점검",
                (
                    f"*⚪ 참고 | {ticker} Backtest 샘플*\n"
                    "• ⚪ 수동 실행 시 수익률/차트 리포트 생성\n"
                    "• ⚪ Slack과 artifact 업로드 경로 확인용"
                ),
                ctx,
            ),
        ),
    ]


def send_message(webhook: str, text: str, blocks: list) -> tuple[bool, str]:
    response = requests.post(webhook, json={"text": text, "blocks": blocks}, timeout=15)
    if response.status_code != 200:
        return False, f"Slack HTTP {response.status_code}: {response.text[:120]}"
    return True, "sent"


def main() -> int:
    parser = argparse.ArgumentParser(description="Send representative Slack messages for every alert type.")
    parser.add_argument("--delay", type=float, default=0.8, help="Seconds to wait between Slack messages.")
    args = parser.parse_args()

    webhook = os.environ.get("SLACK_WEBHOOK_URL") or os.environ.get("MARKET_SCAN_WEBHOOK")
    if not webhook:
        print("Slack webhook missing; set SLACK_WEBHOOK_URL or MARKET_SCAN_WEBHOOK")
        return 1

    config = load_monitor_config()
    failures = []
    for idx, (text, blocks) in enumerate(build_smoke_messages(config), start=1):
        ok, detail = send_message(webhook, text, blocks)
        print(f"[{idx}] {text}: {detail}")
        if not ok:
            failures.append(text)
        time.sleep(max(args.delay, 0))

    if failures:
        print("Failed messages: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
