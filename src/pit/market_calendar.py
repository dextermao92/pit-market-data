"""The US equity market calendar as data: which days NYSE is open, its hours, half days and holidays.

One row per calendar day from 2016 to as far ahead as exchange_calendars knows (about a year), written to
data/curated/market_calendar.parquet by eod and read by dbt (`market_calendar`). The same source already drives
the intraday schedules (pit.market_hours / MarketHoursTimetable); this makes it visible to research, the nightly
jobs and the daily summary.
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

from pit.market_hours import calendar
from pit.storage import write_parquet

START = date(2016, 1, 1)
NY = "America/New_York"
REGULAR_HOURS = 6.5
HOLIDAYS_ZH = {
    "New Year's Day": "元旦", "Dr. Martin Luther King Jr. Day": "马丁·路德·金纪念日", "President's Day": "总统日",
    "Good Friday": "耶稣受难日", "Memorial Day": "阵亡将士纪念日", "Juneteenth National Independence Day": "六月节",
    "July 4th": "独立日", "Labor Day": "劳动节", "Thanksgiving": "感恩节", "Christmas": "圣诞节",
}
SPECIAL_CLOSURES = {date(2018, 12, 5): "老布什国葬", date(2025, 1, 9): "卡特国葬"}
WEEKDAYS_ZH = "一二三四五六日"


def _holiday_names(start: date, end: date) -> dict[date, str]:
    cal = calendar()
    names = cal.regular_holidays.holidays(pd.Timestamp(start), pd.Timestamp(end), return_name=True)
    out = {d.date(): n for d, n in names.items()}
    out.update({d.date(): "special closure" for d in cal.adhoc_holidays if start <= d.date() <= end})
    return out


def table(start: date = START, end: date | None = None) -> pl.DataFrame:
    """One row per calendar day: is_session, open/close in New York time and UTC, hours, early_close, the holiday
    name on a weekday without a session, and the previous / next session."""
    cal = calendar()
    end = min(end or cal.last_session.date(), cal.last_session.date())
    sessions = cal.sessions_in_range(pd.Timestamp(max(start, cal.first_session.date())), pd.Timestamp(end))
    opens, closes = cal.opens.loc[sessions], cal.closes.loc[sessions]
    s = pl.DataFrame({
        "day": [d.date() for d in sessions],
        "open_utc": [t.to_pydatetime() for t in opens], "close_utc": [t.to_pydatetime() for t in closes],
    })
    names = _holiday_names(start, end)
    days = pl.DataFrame({"day": pl.date_range(start, end, "1d", eager=True)})
    t = (days.join(s, on="day", how="left")
         .with_columns(is_session=pl.col("open_utc").is_not_null(), weekday=pl.col("day").dt.weekday(),
                       open_et=pl.col("open_utc").dt.convert_time_zone(NY).dt.time(),
                       close_et=pl.col("close_utc").dt.convert_time_zone(NY).dt.time(),
                       hours=((pl.col("close_utc") - pl.col("open_utc")).dt.total_minutes() / 60))
         .with_columns(early_close=pl.col("is_session") & (pl.col("hours") < REGULAR_HOURS),
                       holiday=pl.when(~pl.col("is_session") & (pl.col("weekday") <= 5))
                       .then(pl.col("day").replace_strict(names, default="unknown closure", return_dtype=pl.String))))
    t = t.with_columns(holiday_zh=pl.col("holiday").replace_strict(
        {**HOLIDAYS_ZH, "special closure": None}, default=None, return_dtype=pl.String))
    t = t.with_columns(holiday_zh=pl.when(pl.col("day").is_in(list(SPECIAL_CLOSURES)))
                       .then(pl.col("day").replace_strict(SPECIAL_CLOSURES, default=None, return_dtype=pl.String))
                       .otherwise(pl.col("holiday_zh")))
    session_day = pl.when(pl.col("is_session")).then(pl.col("day"))
    return t.with_columns(
        prev_session=session_day.shift(1).forward_fill(), next_session=session_day.shift(-1).backward_fill(),
    ).select("day", "weekday", "is_session", "early_close", "open_et", "close_et", "hours", "holiday",
             "holiday_zh", "open_utc", "close_utc", "prev_session", "next_session")


def label(day: date, cal_table: pl.DataFrame | None = None) -> str:
    """Chinese one-liner for a day: regular / half day with hours, weekend, or holiday."""
    t = cal_table if cal_table is not None else table(day - timedelta(days=7), day + timedelta(days=14))
    r = t.filter(pl.col("day") == day).row(0, named=True)
    wd = f"周{WEEKDAYS_ZH[r['weekday'] - 1]}"
    if r["is_session"]:
        kind = "半日市" if r["early_close"] else "正常交易日"
        return f"{day:%m-%d} {wd} {kind} {r['open_et']:%H:%M}–{r['close_et']:%H:%M}（{r['hours']:g} 小时）"
    if r["holiday"]:
        return f"{day:%m-%d} {wd} 休市（{r['holiday_zh'] or r['holiday']}）"
    return f"{day:%m-%d} {wd} 周末"


def upcoming(day: date, days: int = 14) -> list[str]:
    """Labels of the holidays and half days in the `days` after `day`."""
    t = table(day - timedelta(days=7), day + timedelta(days=days + 7))
    special = t.filter((pl.col("day") > day) & (pl.col("day") <= day + timedelta(days=days))
                       & (pl.col("early_close") | pl.col("holiday").is_not_null()))
    return [label(d, t) for d in special["day"]]


def is_session(day: date) -> bool:
    return bool(calendar().is_session(pd.Timestamp(day)))


def next_session(day: date) -> date:
    return calendar().date_to_session(pd.Timestamp(day + timedelta(days=1)), "next").date()


def last_session_of_week(day: date) -> bool:
    """`day` is a session and the next one falls in a later week: the week's weekly jobs run today (a Friday,
    or the Thursday before Good Friday)."""
    return is_session(day) and next_session(day).isocalendar()[:2] != day.isocalendar()[:2]


def run(data_dir: Path) -> Path:
    return write_parquet(table(), data_dir / "curated" / "market_calendar.parquet")


if __name__ == "__main__":
    print(run(Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "data"))))
