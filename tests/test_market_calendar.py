import unittest
from datetime import datetime, timezone

from market_calendar import get_market_state, get_nyse_session, route_market_scan, route_monitor


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


class MarketCalendarTest(unittest.TestCase):
    def test_summer_close_routes_one_hour_after_nyse_close(self):
        decision = route_monitor(utc("2026-07-06T21:00:00+00:00"))

        self.assertTrue(decision.should_run)
        self.assertEqual(decision.mode, "close")
        self.assertEqual(decision.dispatch_key, "close:2026-07-06")
        self.assertEqual(decision.market_close_utc, "2026-07-06T20:00:00+00:00")

    def test_winter_close_routes_one_hour_after_nyse_close(self):
        decision = route_monitor(utc("2026-11-30T22:00:00+00:00"))

        self.assertTrue(decision.should_run)
        self.assertEqual(decision.mode, "close")
        self.assertEqual(decision.dispatch_key, "close:2026-11-30")
        self.assertEqual(decision.market_close_utc, "2026-11-30T21:00:00+00:00")

    def test_thanksgiving_holiday_skips_market_jobs(self):
        self.assertIsNone(get_nyse_session(utc("2026-11-26T15:00:00+00:00").date()))

        monitor = route_monitor(utc("2026-11-26T15:00:00+00:00"))
        scan = route_market_scan(utc("2026-11-26T22:00:00+00:00"))

        self.assertFalse(monitor.should_run)
        self.assertFalse(scan.should_run)

    def test_black_friday_early_close_offsets_all_post_close_jobs(self):
        session = get_nyse_session(utc("2026-11-27T15:00:00+00:00").date())

        self.assertTrue(session.is_early_close)
        self.assertEqual(session.market_close.isoformat(), "2026-11-27T18:00:00+00:00")
        self.assertEqual(route_monitor(utc("2026-11-27T19:00:00+00:00")).mode, "close")
        self.assertEqual(route_monitor(utc("2026-11-27T19:30:00+00:00")).mode, "morning")
        self.assertEqual(route_market_scan(utc("2026-11-27T20:00:00+00:00")).mode, "market_scan")

    def test_market_state_uses_extended_hours_and_new_york_trading_date(self):
        self.assertEqual(get_market_state(utc("2026-07-06T13:00:00+00:00")), "PRE")
        self.assertEqual(get_market_state(utc("2026-07-06T14:00:00+00:00")), "REGULAR")
        self.assertEqual(get_market_state(utc("2026-11-28T00:30:00+00:00")), "POST")
        self.assertEqual(get_market_state(utc("2026-11-28T01:30:00+00:00")), "CLOSED")


if __name__ == "__main__":
    unittest.main()
