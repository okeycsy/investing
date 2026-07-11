from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo


UTC = timezone.utc
NY_TZ = ZoneInfo("America/New_York")
ASIA_TZ = timezone(timedelta(hours=9))

PREMARKET_OPEN_ET = time(4, 0)
POSTMARKET_CLOSE_ET = time(20, 0)

MONITOR_GRACE = timedelta(minutes=29)
NORMAL_GRACE_MINUTES = 20
CLOSE_DELAY = timedelta(minutes=60)
MORNING_DELAY = timedelta(minutes=90)
MARKET_SCAN_DELAY = timedelta(minutes=120)


@dataclass(frozen=True)
class MarketSession:
    session_date: date
    market_open: datetime
    market_close: datetime
    premarket_open: datetime
    postmarket_close: datetime
    source: str = "pandas_market_calendars"

    @property
    def is_early_close(self) -> bool:
        return self.market_close.astimezone(NY_TZ).time() < time(16, 0)


@dataclass(frozen=True)
class DispatchDecision:
    should_run: bool
    mode: str = "skip"
    dispatch_key: str = ""
    reason: str = ""
    session_date: str = ""
    market_close_utc: str = ""


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_utc(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    return ensure_utc(datetime.fromisoformat(cleaned))


def _as_utc(value) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    return ensure_utc(value)


@lru_cache(maxsize=1)
def _nyse_calendar():
    try:
        import pandas_market_calendars as mcal

        return mcal.get_calendar("NYSE")
    except Exception:
        return None


def _fallback_session(session_date: date) -> MarketSession | None:
    if session_date.weekday() >= 5:
        return None
    open_ny = datetime.combine(session_date, time(9, 30), NY_TZ)
    close_ny = datetime.combine(session_date, time(16, 0), NY_TZ)
    return MarketSession(
        session_date=session_date,
        market_open=open_ny.astimezone(UTC),
        market_close=close_ny.astimezone(UTC),
        premarket_open=datetime.combine(session_date, PREMARKET_OPEN_ET, NY_TZ).astimezone(UTC),
        postmarket_close=datetime.combine(session_date, POSTMARKET_CLOSE_ET, NY_TZ).astimezone(UTC),
        source="weekday_fallback",
    )


@lru_cache(maxsize=512)
def get_nyse_session(session_date: date) -> MarketSession | None:
    calendar = _nyse_calendar()
    if calendar is None:
        return _fallback_session(session_date)

    schedule = calendar.schedule(
        start_date=session_date.isoformat(),
        end_date=session_date.isoformat(),
    )
    if schedule.empty:
        return None

    row = schedule.iloc[0]
    open_utc = _as_utc(row["market_open"])
    close_utc = _as_utc(row["market_close"])
    return MarketSession(
        session_date=session_date,
        market_open=open_utc,
        market_close=close_utc,
        premarket_open=datetime.combine(session_date, PREMARKET_OPEN_ET, NY_TZ).astimezone(UTC),
        postmarket_close=datetime.combine(session_date, POSTMARKET_CLOSE_ET, NY_TZ).astimezone(UTC),
    )


def session_for_utc(now_utc: datetime) -> MarketSession | None:
    now_utc = ensure_utc(now_utc)
    ny_date = now_utc.astimezone(NY_TZ).date()
    return get_nyse_session(ny_date)


def trading_date_for_utc(now_utc: datetime) -> date:
    return ensure_utc(now_utc).astimezone(NY_TZ).date()


def get_market_state(now_utc: datetime | None = None) -> str:
    now_utc = ensure_utc(now_utc or datetime.now(UTC))
    session = session_for_utc(now_utc)
    if session is None:
        return "CLOSED"
    if session.premarket_open <= now_utc < session.market_open:
        return "PRE"
    if session.market_open <= now_utc < session.market_close:
        return "REGULAR"
    if session.market_close <= now_utc < session.postmarket_close:
        return "POST"
    return "CLOSED"


def _due(now_utc: datetime, target: datetime, grace: timedelta = MONITOR_GRACE) -> bool:
    now_utc = ensure_utc(now_utc)
    target = ensure_utc(target)
    return target <= now_utc < target + grace


def _asia_target_due(now_utc: datetime, weekday: int, hour: int, minute: int) -> bool:
    local = ensure_utc(now_utc).astimezone(ASIA_TZ)
    target = datetime.combine(local.date(), time(hour, minute), ASIA_TZ)
    return local.weekday() == weekday and _due(now_utc, target.astimezone(UTC))


def route_monitor(now_utc: datetime | None = None) -> DispatchDecision:
    now_utc = ensure_utc(now_utc or datetime.now(UTC))

    if _asia_target_due(now_utc, weekday=0, hour=8, minute=0):
        week = now_utc.astimezone(ASIA_TZ).strftime("%G-W%V")
        return DispatchDecision(True, "weekly", f"weekly:{week}", "Asia Monday 08:00 weekly briefing")

    if _asia_target_due(now_utc, weekday=5, hour=19, minute=0):
        local_date = now_utc.astimezone(ASIA_TZ).date().isoformat()
        return DispatchDecision(True, "13f", f"13f:{local_date}", "Asia Saturday 19:00 13F scan")

    session = session_for_utc(now_utc)
    if session is None:
        return DispatchDecision(False, reason="No NYSE trading session for current New York date")

    session_key = session.session_date.isoformat()
    if _due(now_utc, session.market_close + CLOSE_DELAY):
        return DispatchDecision(
            True,
            "close",
            f"close:{session_key}",
            "NYSE close + 60 minutes",
            session_key,
            session.market_close.isoformat(),
        )

    if _due(now_utc, session.market_close + MORNING_DELAY):
        return DispatchDecision(
            True,
            "morning",
            f"morning:{session_key}",
            "NYSE close + 90 minutes",
            session_key,
            session.market_close.isoformat(),
        )

    market_state = get_market_state(now_utc)
    if market_state in {"PRE", "REGULAR", "POST"} and now_utc.minute < NORMAL_GRACE_MINUTES:
        hour_key = now_utc.strftime("%H")
        return DispatchDecision(
            True,
            "normal",
            f"normal:{session_key}:{hour_key}",
            f"Hourly extended-session monitor ({market_state})",
            session_key,
            session.market_close.isoformat(),
        )

    return DispatchDecision(
        False,
        reason=f"No monitor mode due now; market_state={market_state}",
        session_date=session_key,
        market_close_utc=session.market_close.isoformat(),
    )


def route_market_scan(now_utc: datetime | None = None) -> DispatchDecision:
    now_utc = ensure_utc(now_utc or datetime.now(UTC))
    session = session_for_utc(now_utc)
    if session is None:
        return DispatchDecision(False, reason="No NYSE trading session for current New York date")

    session_key = session.session_date.isoformat()
    if _due(now_utc, session.market_close + MARKET_SCAN_DELAY):
        return DispatchDecision(
            True,
            "market_scan",
            f"market_scan:{session_key}",
            "NYSE close + 120 minutes",
            session_key,
            session.market_close.isoformat(),
        )

    return DispatchDecision(
        False,
        reason="Market scan is not due yet",
        session_date=session_key,
        market_close_utc=session.market_close.isoformat(),
    )


def write_github_output(path: str | None, decision: DispatchDecision) -> None:
    if not path:
        return
    output = Path(path)
    lines = {
        "should_run": "true" if decision.should_run else "false",
        "mode": decision.mode,
        "dispatch_key": decision.dispatch_key,
        "reason": decision.reason,
        "session_date": decision.session_date,
        "market_close_utc": decision.market_close_utc,
    }
    with output.open("a", encoding="utf-8") as fh:
        for key, value in lines.items():
            safe_value = str(value).replace("\n", " ").replace("\r", " ")
            fh.write(f"{key}={safe_value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Route scheduled jobs using the NYSE market calendar.")
    parser.add_argument("kind", choices=["monitor", "market_scan"])
    parser.add_argument("--now-utc", default="", help="ISO UTC timestamp for testing.")
    parser.add_argument("--github-output", default="", help="Path to GitHub Actions output file.")
    args = parser.parse_args()

    now_utc = parse_utc(args.now_utc)
    decision = route_monitor(now_utc) if args.kind == "monitor" else route_market_scan(now_utc)
    write_github_output(args.github_output, decision)
    status = "RUN" if decision.should_run else "SKIP"
    print(
        f"{status} kind={args.kind} mode={decision.mode} key={decision.dispatch_key or '-'} "
        f"session={decision.session_date or '-'} close={decision.market_close_utc or '-'} "
        f"reason={decision.reason}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
