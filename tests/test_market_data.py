import unittest

import market_data


class FakeResponse:
    status_code = 200
    text = "ok"

    def __init__(self):
        self.request_headers = {}

    def json(self):
        return {"ok": True}


class MarketDataTest(unittest.TestCase):
    def test_safe_get_adds_default_user_agent(self):
        original_get = market_data.requests.get
        captured = {}

        def fake_get(url, headers=None, params=None, timeout=15):
            captured["headers"] = headers
            return FakeResponse()

        try:
            market_data.requests.get = fake_get
            resp = market_data.safe_get("https://example.com")
        finally:
            market_data.requests.get = original_get

        self.assertIsNotNone(resp)
        self.assertEqual(captured["headers"]["User-Agent"], market_data.BROWSER_UA)

    def test_fetch_yahoo_chart_tries_second_host_before_yfinance(self):
        original_safe_get = market_data.safe_get
        original_fallback = market_data.fetch_yfinance_chart
        calls = []

        def fake_safe_get(url, headers=None, params=None, timeout=15, retries=3):
            calls.append(url)
            return FakeResponse() if "query2" in url else None

        try:
            market_data.safe_get = fake_safe_get
            market_data.fetch_yfinance_chart = lambda ticker, params: self.fail("fallback should not run")
            resp = market_data.fetch_yahoo_chart("VRT", {"interval": "1d", "range": "5d"})
        finally:
            market_data.safe_get = original_safe_get
            market_data.fetch_yfinance_chart = original_fallback

        self.assertIsNotNone(resp)
        self.assertIn("query1.finance.yahoo.com", calls[0])
        self.assertIn("query2.finance.yahoo.com", calls[1])


if __name__ == "__main__":
    unittest.main()
