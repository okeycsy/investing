import unittest

import market_scan as ms


def synthetic_ohlcv(days: int = 45) -> dict:
    closes = [100 + i * 0.6 for i in range(days)]
    highs = [c + 1.5 for c in closes]
    lows = [c - 1.5 for c in closes]
    volumes = [1_000_000 + i * 5_000 for i in range(days)]
    return {
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
    }


class MarketScanScoringTest(unittest.TestCase):
    def test_sector_for_configured_vrt_comes_from_profile(self):
        self.assertEqual(ms.sector_for_ticker("VRT"), "Industrials")
        self.assertEqual(ms.sector_for_ticker("AAPL"), "Technology")
        self.assertEqual(ms.sector_for_ticker("NO_SUCH_TICKER"), "Unknown")

    def test_score_ticker_returns_error_for_missing_data(self):
        ts = ms.score_ticker("VRT", "Industrials", {})

        self.assertTrue(ts.error)

    def test_score_ticker_bounds_score_and_sets_macro_context(self):
        ts = ms.score_ticker(
            "VRT",
            "Industrials",
            synthetic_ohlcv(),
            btc_above_sma20=True,
            vix=18.2,
        )

        self.assertFalse(ts.error)
        self.assertEqual(ts.ticker, "VRT")
        self.assertEqual(ts.sector, "Industrials")
        self.assertGreaterEqual(ts.score, 0)
        self.assertLessEqual(ts.score, 100)
        self.assertEqual(ts.multiplier, 1.2)
        self.assertTrue(ts.btc_above)
        self.assertEqual(ts.vix, 18.2)
        self.assertIn(ts.grade, {"Strong Buy", "Buy", "Neutral", "Caution", "Avoid"})


if __name__ == "__main__":
    unittest.main()
