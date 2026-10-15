"""Run slots inside NYSE sessions, from exchange_calendars (holidays, half days, DST).

A slot is a run time t = session open + start_offset + k * every, for t < session close + end_offset.
Pure functions: pit.airflow_timetable is a thin Airflow wrapper, so this is testable without Airflow.
"""

from datetime import datetime, timedelta, timezone
from functools import cache

import exchange_calendars as xcals
import pandas as pd


@cache
def calendar():
    return xcals.get_calendar("XNYS")


def next_slot(after: datetime, every: timedelta, start_offset: timedelta, end_offset: timedelta) -> datetime:
    """First slot strictly after `after` (tz-aware)."""
    after = datetime.fromtimestamp(after.timestamp(), timezone.utc)  # Airflow passes pendulum DateTimes
    cal = calendar()
    first = cal.date_to_session(pd.Timestamp(after).tz_convert("America/New_York").date(), "next")
    for session in cal.sessions_in_range(first, first + pd.Timedelta(days=14)):
        t = cal.session_open(session).to_pydatetime() + start_offset
        end = cal.session_close(session).to_pydatetime() + end_offset
        if after >= t:  # jump to the first slot after `after` in this session
            t += ((after - t) // every + 1) * every
        if t < end:
            return t
    raise RuntimeError(f"no session within 14 days after {after}")


def recent_sessions(now: datetime, n: int) -> list:
    """The last n sessions whose close is at or before `now`, oldest first."""
    cal = calendar()
    last = cal.date_to_session(pd.Timestamp(now).tz_convert("America/New_York").date(), "previous")
    if cal.session_close(last) > pd.Timestamp(now):
        last = cal.previous_session(last)
    return [s.date() for s in cal.sessions_window(last, -n)]
