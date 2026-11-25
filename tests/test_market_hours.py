from datetime import datetime, timedelta, timezone

import pytest

from pit.market_hours import next_slot

UTC = timezone.utc
M = timedelta(minutes=1)
OPTIONS = dict(every=5 * M, start_offset=0 * M, end_offset=0 * M)      # 09:30 .. 15:55 ET
EQUITY = dict(every=5 * M, start_offset=20 * M, end_offset=30 * M)     # 09:50 .. 16:25 ET


def utc(*a):
    return datetime(*a, tzinfo=UTC)


@pytest.mark.parametrize("after, kw, expected", [
    (utc(2026, 10, 14, 13, 0), OPTIONS, utc(2026, 10, 14, 13, 30)),     # 09:00 EDT -> open
    (utc(2026, 10, 14, 13, 30), OPTIONS, utc(2026, 10, 14, 13, 35)),    # strictly after
    (utc(2026, 10, 14, 13, 31), OPTIONS, utc(2026, 10, 14, 13, 35)),
    (utc(2026, 10, 14, 19, 55), OPTIONS, utc(2026, 10, 15, 13, 30)),    # 15:55 was the last slot
    (utc(2026, 10, 16, 21, 0), OPTIONS, utc(2026, 10, 19, 13, 30)),     # Friday evening -> Monday
    (utc(2026, 10, 14, 13, 0), EQUITY, utc(2026, 10, 14, 13, 50)),      # first bar fetchable 09:50
    (utc(2026, 10, 14, 20, 25), EQUITY, utc(2026, 10, 15, 13, 50)),     # 16:25 was the last slot
    (utc(2026, 11, 25, 21, 0), OPTIONS, utc(2026, 11, 27, 14, 30)),     # Thanksgiving skipped; EST now
    (utc(2026, 11, 27, 17, 50), OPTIONS, utc(2026, 11, 27, 17, 55)),    # half day: 12:55 last slot
    (utc(2026, 11, 27, 17, 55), OPTIONS, utc(2026, 11, 30, 14, 30)),
    (utc(2026, 11, 27, 18, 20), EQUITY, utc(2026, 11, 27, 18, 25)),     # half day: equity until 13:25
    (utc(2026, 11, 27, 18, 25), EQUITY, utc(2026, 11, 30, 14, 50)),
])
def test_next_slot(after, kw, expected):
    assert next_slot(after, **kw) == expected


def test_accepts_datetime_subclasses():
    # Airflow hands the Timetable pendulum DateTimes; subtracting those from datetimes broke `//`.
    class Sub(datetime):
        def __sub__(self, other):
            return object()  # anything but a timedelta
    assert next_slot(Sub(2026, 10, 14, 13, 31, tzinfo=UTC), **OPTIONS) == utc(2026, 10, 14, 13, 35)


def test_recent_sessions_only_completed():
    from datetime import date
    from pit.market_hours import recent_sessions
    # Thu 15 Oct 2026 17:30 ET: today's session is complete
    assert recent_sessions(utc(2026, 10, 15, 21, 30), 3) == [date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)]
    # 10:00 ET the same day: today not complete yet
    assert recent_sessions(utc(2026, 10, 15, 14, 0), 2) == [date(2026, 10, 13), date(2026, 10, 14)]
    # day after Thanksgiving 2026, 18:00 ET: the half day counts, Thanksgiving doesn't
    assert recent_sessions(utc(2026, 11, 27, 23, 0), 3) == [date(2026, 11, 24), date(2026, 11, 25), date(2026, 11, 27)]


@pytest.mark.parametrize("now, start, end", [
    (utc(2026, 11, 25, 23, 0), utc(2026, 11, 27, 13, 0), utc(2026, 11, 27, 18, 30)),   # Wed 18:00 ET -> Fri (half day), Thanksgiving skipped
    (utc(2026, 11, 27, 15, 0), utc(2026, 11, 27, 13, 0), utc(2026, 11, 27, 18, 30)),   # inside Friday's window
    (utc(2026, 11, 29, 1, 0), utc(2026, 11, 30, 13, 0), utc(2026, 11, 30, 21, 30)),    # Sat 20:00 ET -> Monday 08:00-16:30
    (utc(2026, 7, 10, 23, 0), utc(2026, 7, 13, 12, 0), utc(2026, 7, 13, 20, 30)),      # EDT: 08:00 ET = 12:00 UTC
])
def test_collection_window(now, start, end):
    from pit.market_hours import collection_window
    assert collection_window(now) == (start, end)
