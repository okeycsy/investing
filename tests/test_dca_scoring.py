import unittest

import dca_scoring
import hood_monitor as hm


def synthetic_ohlcv(days: int = 220) -> dict:
    closes = []
    for idx in range(days):
        base = 100 + idx * 0.18
        pullback = max(0, idx - (days - 12)) * 0.45
        closes.append(round(base - pullback, 2))
    highs = [round(c + 1.2, 2) for c in closes]
    lows = [round(c - 1.1, 2) for c in closes]
    opens = [round(c - 0.2, 2) for c in closes]
    volumes = [1_000_000 + (idx % 17) * 25_000 for idx in range(days)]
    return {
        "closes": closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
    }


class DCAScoringTest(unittest.TestCase):
    def test_dca_score_builds_all_layers(self):
        daily = synthetic_ohlcv()
        weekly = synthetic_ohlcv(45)

        score = dca_scoring.calculate_dca_technical_score(daily, weekly)

        self.assertIsNotNone(score)
        self.assertEqual([layer.layer_id for layer in score.layers], ["A", "B", "C", "D", "E"])
        self.assertGreaterEqual(score.total, 0)
        self.assertLessEqual(score.total, 100)
        self.assertIn(score.grade, {"Strong Buy", "Buy", "Neutral", "Caution", "Avoid"})

    def test_hood_monitor_keeps_compatibility_wrapper(self):
        daily = synthetic_ohlcv()
        score = hm.calculate_dca_technical_score(daily, synthetic_ohlcv(45))

        self.assertIsNotNone(score)
        self.assertEqual(score.total, dca_scoring.calculate_dca_technical_score(daily, synthetic_ohlcv(45)).total)


if __name__ == "__main__":
    unittest.main()
