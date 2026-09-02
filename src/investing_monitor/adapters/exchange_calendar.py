from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone

import exchange_calendars as exchange_calendars
import pandas as pd

from investing_monitor.domain.models import MarketSession
from investing_monitor.runtime.tick import NEW_YORK


@dataclass(frozen=True)
class SessionWindow:
    trading_date: date
    open_at: datetime
    close_at: datetime


class XNYSCalendar:
    def __init__(self) -> None:
        self._calendar = exchange_calendars.get_calendar("XNYS")

    def is_trading_day(self, value: date) -> bool:
        return bool(self._calendar.is_session(pd.Timestamp(value)))

    def regular_open(self, value: date) -> datetime:
        return self._calendar.session_open(pd.Timestamp(value)).to_pydatetime()

    def regular_close(self, value: date) -> datetime:
        return self._calendar.session_close(pd.Timestamp(value)).to_pydatetime()

    def window(self, value: date) -> SessionWindow:
        if not self.is_trading_day(value):
            raise ValueError(f"{value.isoformat()} is not an XNYS trading day")
        return SessionWindow(value, self.regular_open(value), self.regular_close(value))

    def previous_trading_day(self, value: date) -> date:
        session = self._calendar.date_to_session(
            pd.Timestamp(value),
            direction="previous" if not self.is_trading_day(value) else "none",
        )
        if self.is_trading_day(value):
            session = self._calendar.previous_session(session)
        return session.date()

    def session_at(self, observed_at: datetime) -> MarketSession:
        observed_at = _utc(observed_at)
        local = observed_at.astimezone(NEW_YORK)
        if not self.is_trading_day(local.date()):
            return MarketSession.CLOSED
        window = self.window(local.date())
        pre_open = datetime.combine(local.date(), time(4, 0), NEW_YORK).astimezone(
            timezone.utc
        )
        post_close = datetime.combine(local.date(), time(20, 0), NEW_YORK).astimezone(
            timezone.utc
        )
        if pre_open <= observed_at < window.open_at:
            return MarketSession.PRE
        if window.open_at <= observed_at < window.close_at:
            return MarketSession.REGULAR
        if window.close_at <= observed_at < post_close:
            return MarketSession.POST
        return MarketSession.CLOSED

    def is_extended_session(self, observed_at: datetime) -> bool:
        return self.session_at(observed_at) is not MarketSession.CLOSED


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("calendar timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
