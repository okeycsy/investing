import unittest
import xml.etree.ElementTree as ET
from datetime import date
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
