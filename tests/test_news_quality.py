import unittest

import hood_monitor as hm
from monitor_config import load_monitor_config


class NewsQualityTest(unittest.TestCase):
    def test_vrt_profile_loads_news_terms(self):
        config = load_monitor_config()

        self.assertEqual(config.ticker, "VRT")
        self.assertEqual(config.sector, "Industrials")
        self.assertIn("Vertiv", config.news_terms)
        self.assertIn("data center cooling", config.news_terms)

    def test_keyword_matched_news_becomes_candidate(self):
        news = [{
            "title": "Vertiv rises as data center cooling demand accelerates",
            "body": "",
            "source": "Example",
            "hash": "n1",
        }]

        hm.annotate_news_candidates(news)
        candidates = hm.news_candidate_items(news)

        self.assertEqual(len(candidates), 1)
        self.assertIn("Vertiv", candidates[0]["keyword_matches"])
        self.assertTrue(candidates[0]["candidate_summary"])

    def test_relevant_news_is_not_duplicated_as_candidate(self):
        news = [{
            "title": "Vertiv announces new liquid cooling system",
            "body": "",
            "source": "Example",
            "hash": "n2",
            "summary": "냉각 신제품",
            "sentiment": "positive",
        }]

        hm.annotate_news_candidates(news)

        self.assertEqual(len(hm.news_relevant_items(news)), 1)
        self.assertEqual(hm.news_candidate_items(news), [])


if __name__ == "__main__":
    unittest.main()
