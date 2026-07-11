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
        self.assertIn("주가 전일 대비 +5.2%", text)
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


if __name__ == "__main__":
    unittest.main()
