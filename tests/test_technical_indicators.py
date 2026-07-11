import unittest

import technical_indicators as ti


class TechnicalIndicatorsTest(unittest.TestCase):
    def test_rsi_handles_monotonic_gain_and_flat_short_series(self):
        self.assertEqual(ti.calculate_rsi([100, 101, 102], period=14), 50.0)
        self.assertEqual(ti.calculate_rsi(list(range(1, 30))), 100.0)

    def test_macd_and_signal_flags_are_stable(self):
        closes = [100 + idx * 0.6 for idx in range(45)]

        macd_line, macd_signal, macd_hist = ti.calculate_macd(closes)
        signals = ti.get_technical_signals(closes)

        self.assertGreater(macd_line, 0)
        self.assertGreater(macd_signal, 0)
        self.assertGreater(macd_hist, 0)
        self.assertEqual(signals.rsi_alert, "overbought")

    def test_flow_helpers_return_expected_shapes(self):
        closes = [10, 11, 10.5, 12, 11.5, 12.5, 13, 12.8, 13.2, 13.8, 14, 14.4, 14.1, 14.8, 15.2, 15.6, 15.4, 16]
        highs = [c + 0.5 for c in closes]
        lows = [c - 0.5 for c in closes]
        volumes = [1000 + idx * 50 for idx in range(len(closes))]

        self.assertEqual(len(ti.calc_obv(closes, volumes)), len(closes))
        self.assertIsNotNone(ti.calc_mfi(highs, lows, closes, volumes))
        self.assertIsNotNone(ti.calc_atr(highs, lows, closes))
        self.assertIsNotNone(ti.calc_cmf(highs, lows, closes, volumes, period=10))


if __name__ == "__main__":
    unittest.main()
