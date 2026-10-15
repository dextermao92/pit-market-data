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
