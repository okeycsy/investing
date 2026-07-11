import unittest

import hood_monitor as hm


def section_text(blocks):
    return "\n".join(
        block.get("text", {}).get("text", "")
        for block in blocks
        if block.get("type") == "section"
    )


class AlertQualityTest(unittest.TestCase):
    def test_build_alert_quality_promotes_urgent_reasons(self):
        blocks = hm.build_alert_quality_blocks(
            "close",
            price=hm.PriceData(current=105, prev_close=100, change_pct=5.2, volume=2000, vol_avg_5d=1000),
            technicals=hm.TechnicalSignals(rsi_14=72, macd_alert="bearish_cross"),
            short=hm.ShortInterestData(short_pct=61.5),
            insiders=[hm.InsiderTrade(trade_type="Sale", shares=20_000, price=300, total_value=6_000_000)],
            news=[{"summary": "가이던스 하향", "sentiment": "negative"}],
        )

        text = section_text(blocks)

        self.assertIn("긴급", text)
        self.assertIn("오늘 방향: 양전", text)
        self.assertNotIn("전일 대비", text)
        self.assertNotIn("+5.2%", text)
        self.assertIn("공매도 비중 61.5%", text)
        self.assertIn("신규 내부자 매도", text)

    def test_build_alert_quality_defaults_to_info_when_no_reasons(self):
        blocks = hm.build_alert_quality_blocks("normal")

        text = section_text(blocks)

        self.assertIn("참고", text)
        self.assertIn("새로운 핵심 시그널은 없고", text)

    def test_insert_alert_quality_summary_after_header(self):
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "Header"}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": "Body"}},
        ]

        hm.insert_alert_quality_summary(
            blocks,
            "normal",
            price=hm.PriceData(current=101, prev_close=100, change_pct=1.0),
        )

        self.assertEqual(blocks[0]["type"], "header")
        self.assertEqual(blocks[1]["type"], "section")
        self.assertIn("핵심 요약", blocks[1]["text"]["text"])

    def test_relative_strength_block_hides_price_and_change_numbers(self):
        blocks = hm.format_beta_block({
            "beta": 1.2,
            "qqq_pct": 1.0,
            "expected_pct": 1.2,
            "actual_pct": 2.0,
            "divergence": 0.8,
            "peer_changes": {},
            "peer_avg": 0.0,
            "peer_diff": 0.0,
        })

        text = section_text(blocks)

        self.assertIn("SOXX 대비 아웃퍼폼", text)
        self.assertIn("양전", text)
        self.assertNotIn("2.0", text)
        self.assertNotIn("실제수익률", text)


if __name__ == "__main__":
    unittest.main()
