import unittest

import hood_monitor as hm
from monitor_config import load_monitor_config


class NewsQualityTest(unittest.TestCase):
    def test_vrt_profile_loads_news_terms(self):
        config = load_monitor_config()

        self.assertEqual(config.ticker, "VRT")
        self.assertEqual(config.sector, "Industrials")
        self.assertEqual(config.exchange, "NYSE")
        self.assertEqual(config.benchmark, "SOXX")
        self.assertIn("Vertiv Holdings", config.company_aliases)
        self.assertIn("Vertiv", config.news_terms)
        self.assertIn("data center cooling", config.news_terms)
        self.assertIn("liquid cooling", config.core_products)
        self.assertIn("backlog", config.priority_keywords)
        self.assertIn("margin pressure", config.risk_keywords)
        self.assertIn("markets: data centers", config.profile_context)

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

    def test_priority_keyword_marks_candidate_as_watch(self):
        news = [{
            "title": "Vertiv backlog grows on AI data center liquid cooling demand",
            "body": "",
            "source": "Example",
            "hash": "n_priority",
        }]

        hm.annotate_news_candidates(news)
        candidates = hm.news_candidate_items(news)

        self.assertEqual(candidates[0]["candidate_level"], "watch")
        self.assertIn("backlog", candidates[0]["priority_matches"])

    def test_risk_keyword_marks_candidate_as_watch(self):
        news = [{
            "title": "Vertiv faces margin pressure from tariffs and supply chain delays",
            "body": "",
            "source": "Example",
            "hash": "n_risk",
        }]

        hm.annotate_news_candidates(news)
        candidates = hm.news_candidate_items(news)

        self.assertEqual(candidates[0]["candidate_level"], "watch")
        self.assertIn("margin pressure", candidates[0]["risk_matches"])

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

    def test_candidate_news_is_not_rendered_without_ai_confirmation(self):
        news = [{
            "title": "Vertiv backlog grows on AI data center liquid cooling demand",
            "body": "",
            "source": "Example",
            "hash": "n_candidate",
        }]

        hm.annotate_news_candidates(news)

        self.assertEqual(hm.format_news_block(news), [])

    def test_relevant_news_block_still_renders_ai_confirmed_items(self):
        news = [{
            "title": "Vertiv announces new liquid cooling system",
            "body": "",
            "source": "Example",
            "hash": "n_relevant",
            "summary": "냉각 신제품",
            "translation": "Vertiv가 새 냉각 제품을 공개했습니다.",
            "sentiment": "positive",
        }]

        blocks = hm.format_news_block(news)

        self.assertTrue(blocks)
        self.assertIn("냉각 신제품", blocks[0]["text"]["text"])


if __name__ == "__main__":
    unittest.main()
