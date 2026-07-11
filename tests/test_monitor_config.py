import os
import tempfile
import unittest
from pathlib import Path

from monitor_config import load_monitor_config, resolve_runtime_file


class MonitorConfigTest(unittest.TestCase):
    def setUp(self):
        self.old_env = {
            name: os.environ.get(name)
            for name in (
                "MONITOR_TICKER",
                "MONITOR_PEER_TICKERS",
                "MONITOR_NEWS_KEYWORDS",
                "MONITOR_STATE_DIR",
            )
        }

    def tearDown(self):
        for name, value in self.old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_markdown_config_loads_extended_profile_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "monitor_config.md"
            config_path.write_text(
                "\n".join([
                    "ticker: $ABC",
                    "company_name: ABC Data Power",
                    "company_aliases: ABC Power, ABC Infrastructure",
                    "sector: Industrials",
                    "industry: Critical Power",
                    "peer_tickers: $ETN, $NVT",
                    "end_markets: data centers, telecom",
                    "core_products: UPS systems, liquid cooling",
                    "priority_keywords: backlog, guidance",
                    "risk_keywords: margin pressure, tariffs",
                    "state_dir: tmp_state",
                ]),
                encoding="utf-8",
            )

            config = load_monitor_config(config_path)

        self.assertEqual(config.ticker, "ABC")
        self.assertEqual(config.peer_tickers, ("ETN", "NVT"))
        self.assertIn("ABC Power", config.company_aliases)
        self.assertIn("liquid cooling", config.news_terms)
        self.assertIn("backlog", config.priority_keywords)
        self.assertIn("markets: data centers", config.profile_context)

    def test_environment_overrides_config_values(self):
        os.environ["MONITOR_TICKER"] = "XYZ"
        os.environ["MONITOR_PEER_TICKERS"] = "AAPL, MSFT"
        os.environ["MONITOR_NEWS_KEYWORDS"] = "custom signal, second signal"
        os.environ["MONITOR_STATE_DIR"] = "custom_state"

        config = load_monitor_config()

        self.assertEqual(config.ticker, "XYZ")
        self.assertEqual(config.peer_tickers, ("AAPL", "MSFT"))
        self.assertEqual(config.news_keywords, ("custom signal", "second signal"))
        self.assertTrue(str(resolve_runtime_file(config, "state.json", "NO_SUCH_ENV")).endswith("custom_state/XYZ/state.json"))


if __name__ == "__main__":
    unittest.main()
