"""Airflow Timetable over pit.market_hours: runs only inside NYSE sessions. Registered by plugins/market_hours.py."""

from datetime import timedelta

import pendulum
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable

from pit.market_hours import next_slot


class MarketHoursTimetable(Timetable):
    """A run every `every_minutes` from session open + start_offset to before session close + end_offset."""

    def __init__(self, every_minutes: int = 5, start_offset_minutes: int = 0, end_offset_minutes: int = 0):
        self.params = dict(every_minutes=every_minutes, start_offset_minutes=start_offset_minutes,
                           end_offset_minutes=end_offset_minutes)
        self.every = timedelta(minutes=every_minutes)
        self.offsets = timedelta(minutes=start_offset_minutes), timedelta(minutes=end_offset_minutes)
        self.description = (f"Every {every_minutes} min in NYSE sessions, "
                            f"open{start_offset_minutes:+d} to close{end_offset_minutes:+d} min")

    @property
    def summary(self) -> str:
        return self.description

    def serialize(self) -> dict:
        return self.params

    @classmethod
    def deserialize(cls, data: dict) -> "MarketHoursTimetable":
        return cls(**data)

    def infer_manual_data_interval(self, *, run_after):
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        if last_automated_data_interval is not None:
            after = last_automated_data_interval.end
        elif restriction.earliest is None:
            return None
        else:
            after = restriction.earliest - self.every
            if not restriction.catchup:  # like cron with catchup=False: the latest slot runs right away
                after = max(after, pendulum.now("UTC") - self.every)
        t = pendulum.instance(next_slot(after, self.every, *self.offsets))
        if restriction.latest is not None and t > restriction.latest:
            return None
        return DagRunInfo.interval(start=t - self.every, end=t)
