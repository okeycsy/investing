import tempfile
import unittest
from pathlib import Path

import sec_filings
from monitor_state import default_state, load_state_file, save_state_file
from slack_blocks import chunk_blocks, section_block


class MonitorModulesTest(unittest.TestCase):
    def test_state_roundtrip_uses_default_for_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "state.json"

            state = load_state_file(path, default_state)
            self.assertIn("last_news_hashes", state)
            self.assertIn("dca_history", state)

            state["last_news_hashes"] = ["abc"]
            save_state_file(path, state)

            loaded = load_state_file(path, default_state)
            self.assertEqual(loaded["last_news_hashes"], ["abc"])

    def test_slack_block_helpers_support_fields_and_chunking(self):
        block = section_block("hello", fields=["a", "b"])

        self.assertEqual(block["type"], "section")
        self.assertEqual(len(block["fields"]), 2)
        self.assertEqual([len(chunk) for _, chunk in chunk_blocks(list(range(41)))], [40, 1])

    def test_13f_infotable_extracts_matching_common_stock_and_skips_options(self):
        xml = """
        <informationTable>
          <infoTable>
            <nameOfIssuer>VERTIV HOLDINGS CO</nameOfIssuer>
            <shrsOrPrnAmt><sshPrnamt>12345</sshPrnamt></shrsOrPrnAmt>
            <value>678</value>
          </infoTable>
          <infoTable>
            <nameOfIssuer>VERTIV HOLDINGS CO</nameOfIssuer>
            <shrsOrPrnAmt><sshPrnamt>99999</sshPrnamt></shrsOrPrnAmt>
            <value>111</value>
            <putCall>Put</putCall>
          </infoTable>
        </informationTable>
        """

        shares, value, change_type = sec_filings.extract_ticker_from_infotable(
            xml,
            ("VERTIV", "VRT"),
        )

        self.assertEqual(shares, 12345)
        self.assertEqual(value, 678000.0)
        self.assertEqual(change_type, "REPORTED")


if __name__ == "__main__":
    unittest.main()
