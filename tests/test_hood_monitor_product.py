import unittest
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import hood_monitor as hm


def visible_text(blocks):
    chunks = []
    for block in blocks:
        text = block.get("text")
        if isinstance(text, dict):
            chunks.append(text.get("text", ""))
        for element in block.get("elements", []):
            if isinstance(element, dict):
                chunks.append(element.get("text", ""))
    return "\n".join(chunks)


class ProductContractTest(unittest.TestCase):
    def test_vrt_profile_contains_business_kpis(self):
        self.assertIn("liquid cooling", hm.NEWS_TERMS)
        self.assertIn("backlog", hm.CORE_KPIS)
        self.assertIn("guidance cut", hm.RISK_KEYWORDS)

    def test_relative_block_hides_prices_and_exact_returns(self):
        blocks = hm.format_beta_block({
            "beta": 1.4,
            "qqq_pct": 1.25,
            "expected_pct": 1.75,
            "actual_pct": 2.50,
            "divergence": 0.75,
            "peer_changes": {"ETN": 0.8},
            "peer_avg": 0.8,
            "peer_diff": 1.7,
        })
        text = visible_text(blocks)
        self.assertIn("양전", text)
        self.assertIn("아웃퍼폼", text)
        self.assertIn("반도체 지수(SOXX)", text)
        self.assertIn("피어 평균(동일가중: 이튼)", text)
        self.assertNotIn("표시하지 않습니다", text)
        self.assertNotIn("2.50", text)
        self.assertNotIn("1.25", text)
        self.assertNotIn("$123.45", text)

    def test_news_block_has_source_and_thesis_impact(self):
        blocks = hm.format_news_block([{
            "title": "Vertiv raises guidance",
            "summary": "가이던스 상향",
            "translation": "Vertiv가 연간 가이던스를 상향했습니다.",
            "sentiment": "positive",
            "thesis_impact": "strengthen",
            "confidence": "high",
            "link": "https://example.com/vrt",
            "source": "example.com",
        }])
        text = visible_text(blocks)
        self.assertIn("논지 강화", text)
        self.assertIn("원문", text)
        self.assertIn("https://example.com/vrt", text)

    def test_failed_ai_news_is_not_marked_seen(self):
        items = [{"hash": "a", "analysis_status": "failed"},
                 {"hash": "b", "analysis_status": "success"}]
        self.assertEqual(hm.analyzed_news_hashes(items), ["b"])

    def test_partial_ai_response_retries_omitted_news(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "content": [{
                "type": "text",
                "text": '[{"idx": 1, "relevant": false}]',
            }]
        }
        news = [
            {"title": "first", "link": "", "hash": "a"},
            {"title": "second", "link": "", "hash": "b"},
        ]
        with (
            patch.object(hm, "ANTHROPIC_API_KEY", "test-key"),
            patch.object(hm, "_fetch_article_body", return_value=""),
            patch.object(hm.requests, "post", return_value=response),
        ):
            analyzed = hm.translate_news(news)

        self.assertEqual(analyzed[0]["analysis_status"], "success")
        self.assertEqual(analyzed[1]["analysis_status"], "failed")
        self.assertEqual(hm.analyzed_news_hashes(analyzed), ["a"])
        self.assertIn("부분 실패", hm.SOURCE_HEALTH["AI 뉴스"])

    def test_incomplete_relevant_ai_item_is_retried(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "content": [{
                "type": "text",
                "text": '[{"idx": 1, "relevant": true, "summary": "요약만 있음"}]',
            }]
        }
        news = [{"title": "filing news", "link": "", "hash": "a"}]
        with (
            patch.object(hm, "ANTHROPIC_API_KEY", "test-key"),
            patch.object(hm, "_fetch_article_body", return_value=""),
            patch.object(hm.requests, "post", return_value=response),
        ):
            analyzed = hm.translate_news(news)

        self.assertEqual(analyzed[0]["analysis_status"], "failed")
        self.assertEqual(hm.analyzed_news_hashes(analyzed), [])

    def test_dca_block_is_secondary_context(self):
        score = hm.DCATechnicalScore(total=82, grade="Strong Buy", layers=[])
        text = visible_text(hm.format_dca_technical_block(score))
        self.assertIn("DCA 보조", text)
        self.assertNotIn("Strong Buy", text)
        self.assertNotIn("Avoid", text)

    def test_market_detail_blocks_hide_price_levels(self):
        volume = hm.VolumeProfile(
            poc_price=123.45,
            poc_signal="support",
            vol_30m=100,
            vol_avg_30m=80,
            vol_ratio=1.25,
        )
        safety = hm.SafetyMargin(
            sma20=120.0,
            bb_upper=130.0,
            bb_lower=110.0,
            current_price=123.45,
            bb_signal="normal",
            momentum_signal="stable",
        )
        text = visible_text(
            hm.format_volume_profile_block(volume)
            + hm.format_safety_margin_block(safety)
        )
        for forbidden in ("123.45", "120.00", "130.00", "110.00"):
            self.assertNotIn(forbidden, text)

    def test_close_summary_contains_decision_contract(self):
        price = hm.PriceData(change_pct=-1.2, prev_close=10, current=9.88)
        blocks = hm.build_decision_summary_blocks(
            price=price,
            benchmark_pct=-2.0,
            news=[],
            filings=[],
            source_health={"Yahoo": "정상", "SEC": "정상", "AI": "정상"},
        )
        text = visible_text(blocks)
        self.assertIn("음전", text)
        self.assertIn("아웃퍼폼", text)
        self.assertIn("기존 계획 유지", text)
        self.assertNotIn("-1.2", text)
        self.assertNotIn("-2.0", text)

    def test_volume_activity_uses_prior_20_sessions(self):
        activity = hm.calculate_volume_activity([100] * 20 + [160])

        self.assertIsNotNone(activity)
        self.assertEqual(activity.average_volume, 100)
        self.assertAlmostEqual(activity.ratio, 1.6)
        self.assertTrue(activity.exploded)

    def test_price_alerts_follow_integer_bands_in_one_direction(self):
        self.assertEqual(hm.next_price_alert_level(-4.4), 4)
        self.assertIsNone(hm.next_price_alert_level(-4.7, 4, "down"))
        self.assertEqual(hm.next_price_alert_level(-5.0, 4, "down"), 5)
        self.assertEqual(hm.next_price_alert_level(-7.2, 5, "down"), 7)

    def test_price_alert_does_not_rearm_on_reversal_or_retracement(self):
        self.assertEqual(hm.next_price_alert_level(8.1), 8)
        self.assertIsNone(hm.next_price_alert_level(5.2, 8, "up"))
        self.assertIsNone(hm.next_price_alert_level(-9.1, 8, "up"))

    def test_periodic_sec_filing_is_summarized_and_related_8k_is_deduped(self):
        accession = "0000000000-26-000001"

        def metric(tag, unit, prior, current):
            return tag, {
                "units": {unit: [
                    {
                        "accn": accession,
                        "start": "2025-04-01",
                        "end": "2025-06-30",
                        "val": prior,
                    },
                    {
                        "accn": accession,
                        "start": "2026-04-01",
                        "end": "2026-06-30",
                        "val": current,
                    },
                ]}
            }

        facts = dict([
            metric("RevenueFromContractWithCustomerExcludingAssessedTax", "USD", 2_600, 3_250),
            metric("OperatingIncomeLoss", "USD", 400, 600),
            metric("NetIncomeLoss", "USD", 300, 450),
            metric("EarningsPerShareDiluted", "USD/shares", 0.80, 1.20),
        ])
        company_facts = {"facts": {"us-gaap": facts}}
        ten_q = hm.CompanyFiling(
            form="10-Q",
            filing_date="2026-07-29",
            report_date="2026-06-30",
            accession=accession,
            hash="tenq",
            url="https://sec.example/10q",
        )
        eight_k = hm.CompanyFiling(
            form="8-K",
            filing_date="2026-07-29",
            report_date="2026-07-29",
            accession="0000000000-26-000002",
            hash="eightk",
            items="2.02,7.01,9.01",
            url="https://sec.example/8k",
        )

        analyzed = hm.analyze_company_filings([ten_q, eight_k], company_facts)
        alertable = hm.alertable_company_filings(analyzed)
        text = visible_text(hm.format_company_filings_block(alertable))

        self.assertEqual(alertable, [ten_q])
        self.assertTrue(eight_k.skip)
        self.assertIn("매출", text)
        self.assertIn("전년 동기 대비 +25.0%", text)
        self.assertIn("논지 강화", text)
        self.assertIn("SEC 원문", text)
        self.assertNotIn("내용 확인 필요", text)

    def test_failed_sec_analysis_is_not_marked_seen(self):
        state = {"last_company_filing_hashes": []}
        filing = hm.CompanyFiling(hash="retry", analysis_status="failed")

        hm.remember_company_filings(state, [filing])

        self.assertEqual(state["last_company_filing_hashes"], [])

    def test_workflow_stages_only_existing_state_files(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertIn('if [ -f "$file" ]', workflow)
        self.assertIn('git add -f -- "$file"', workflow)

    def test_volume_below_threshold_is_not_exploded(self):
        activity = hm.calculate_volume_activity([100] * 20 + [149])

        self.assertIsNotNone(activity)
        self.assertFalse(activity.exploded)

    def test_volume_activity_requires_full_baseline(self):
        self.assertIsNone(hm.calculate_volume_activity([100] * 20))

    def test_close_price_uses_latest_daily_volume_and_prior_20_day_average(self):
        today = datetime.now(hm.NY_TZ).date()
        days = [today - timedelta(days=21 - index) for index in range(22)]
        timestamps = [
            int(datetime.combine(day, time(12), tzinfo=hm.NY_TZ).timestamp())
            for day in days
        ]
        response = Mock()
        response.json.return_value = {
            "chart": {
                "result": [{
                    "timestamp": timestamps,
                    "meta": {},
                    "indicators": {
                        "quote": [{
                            "close": [10.0] * 22,
                            "volume": [100] * 21 + [160],
                        }]
                    },
                }]
            }
        }

        with (
            patch.object(hm, "_yahoo_throttle"),
            patch.object(hm, "fetch_yahoo_chart", return_value=response),
        ):
            price = hm.fetch_price(realtime=False)

        self.assertEqual(price.volume, 160)
        self.assertEqual(price.vol_avg_20d, 100)

    def test_volume_block_shows_comparison_and_result(self):
        activity = hm.calculate_volume_activity([100] * 20 + [160])
        text = visible_text(hm.format_volume_activity_block(activity, finalized=True))

        self.assertIn("거래량 터짐", text)
        self.assertIn("당일 거래량", text)
        self.assertIn("20일 평균", text)
        self.assertIn("1.60x", text)

    def test_intraday_volume_block_does_not_claim_final_result(self):
        activity = hm.calculate_volume_activity([100] * 20 + [149])
        text = visible_text(hm.format_volume_activity_block(activity, finalized=False))

        self.assertIn("장중 거래량 기준 미달", text)
        self.assertNotIn("거래량 안 터짐", text)

    def test_volume_explosion_counts_as_important_change(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            volume_activity=hm.calculate_volume_activity([100] * 20 + [160]),
            source_health={"Yahoo": "정상"},
        )

        self.assertIn("중요 변화 1건", visible_text(blocks))

    def test_source_failure_prevents_false_no_change(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            source_health={"Yahoo": "정상", "AI": "실패"},
        )
        text = visible_text(blocks)
        self.assertIn("판정 보류", text)
        self.assertIn("점검 필요", text)
        self.assertNotIn("새로운 중요 사건 없음", text)

    def test_open_market_sale_requires_review(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            insiders=[hm.InsiderTrade(trade_type="Sale")],
            source_health={"Yahoo": "정상", "SEC": "정상"},
        )
        text = visible_text(blocks)
        self.assertIn("점검 필요", text)

    def test_form4_disposition_is_not_labeled_open_market_sale(self):
        transaction = ET.fromstring("""
        <nonDerivativeTransaction>
          <transactionCoding><transactionCode>D</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>100</value></transactionShares>
            <transactionPricePerShare><value>10</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
        </nonDerivativeTransaction>
        """)
        trade = hm._parse_transaction(transaction, "Officer", "CFO", "2026-01-01", "https://sec.example")
        self.assertEqual(trade.trade_type, "Disposition")

    def test_routine_awards_do_not_trigger_immediate_alert(self):
        trades = [
            hm.InsiderTrade(trade_type="Award", shares=10),
            hm.InsiderTrade(trade_type="Disposition", shares=100),
            hm.InsiderTrade(trade_type="Purchase", shares=5),
            hm.InsiderTrade(trade_type="Sale", shares=2_000),
        ]
        material = hm.material_insider_trades(trades)
        self.assertEqual([trade.trade_type for trade in material], ["Purchase", "Sale"])

    def test_stale_insider_sales_do_not_trigger_first_run_alert(self):
        trades = [
            hm.InsiderTrade(trade_type="Sale", shares=2_000, date="2026-06-01"),
            hm.InsiderTrade(trade_type="Sale", shares=2_000, date="2026-08-10"),
        ]
        material = hm.material_insider_trades(
            trades, lookback_days=21, as_of=date(2026, 8, 16)
        )
        self.assertEqual([trade.date for trade in material], ["2026-08-10"])

    def test_first_13f_observation_is_a_baseline(self):
        filing = hm.Filing13F(
            institution="Example Fund",
            shares=100,
            change_type="BASELINE",
            filing_date="2026-08-01",
        )
        text = visible_text(hm.format_13f_block([filing]))
        self.assertIn("기준 설정", text)
        self.assertNotIn("· 신규", text)

    def test_github_run_without_slack_fails(self):
        with patch.object(hm, "SLACK_WEBHOOK", ""), patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}):
            with self.assertRaises(RuntimeError):
                hm.send_slack([])


if __name__ == "__main__":
    unittest.main()
