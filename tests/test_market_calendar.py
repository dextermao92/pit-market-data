from datetime import date, time

import polars as pl
import pytest

from pit import market_calendar as mc
from pit.health import calendar_fields


@pytest.fixture(scope="module")
def cal():
    return mc.table(date(2016, 1, 1), date(2027, 1, 31))


def test_one_row_per_day_with_sessions_and_holidays(cal):
    assert cal["day"].is_unique().all()
    assert cal["day"].min() == date(2016, 1, 1)
    h2026 = cal.filter(pl.col("holiday").is_not_null() & (pl.col("day").dt.year() == 2026))
    assert h2026["holiday_zh"].to_list() == ["元旦", "马丁·路德·金纪念日", "总统日", "耶稣受难日", "阵亡将士纪念日", "六月节",
                                             "独立日", "劳动节", "感恩节", "圣诞节"]
    assert cal.filter(pl.col("is_session") & pl.col("holiday").is_not_null()).height == 0
    assert cal.filter(pl.col("weekday") >= 6)["holiday"].is_null().all()  # weekends are not holidays


def test_thanksgiving_week_2026(cal):
    rows = {r["day"]: r for r in cal.filter(pl.col("day").is_between(date(2026, 11, 25), date(2026, 11, 30))).iter_rows(named=True)}
    assert not rows[date(2026, 11, 26)]["is_session"]
    friday = rows[date(2026, 11, 27)]
    assert friday["early_close"] and (friday["open_et"], friday["close_et"], friday["hours"]) == (time(9, 30), time(13, 0), 3.5)
    assert rows[date(2026, 11, 26)]["prev_session"] == date(2026, 11, 25)
    assert rows[date(2026, 11, 26)]["next_session"] == date(2026, 11, 27)
    assert rows[date(2026, 11, 30)]["hours"] == 6.5 and not rows[date(2026, 11, 30)]["early_close"]


@pytest.mark.parametrize("day, text", [
    (date(2026, 11, 26), "11-26 周四 休市（感恩节）"),
    (date(2026, 11, 27), "11-27 周五 半日市 09:30–13:00（3.5 小时）"),
    (date(2026, 11, 28), "11-28 周六 周末"),
    (date(2026, 11, 30), "11-30 周一 正常交易日 09:30–16:00（6.5 小时）"),
    (date(2018, 12, 5), "12-05 周三 休市（老布什国葬）"),
    (date(2025, 1, 9), "01-09 周四 休市（卡特国葬）"),
])
def test_labels(day, text):
    assert mc.label(day) == text


@pytest.mark.parametrize("day, last", [
    (date(2026, 11, 25), False), (date(2026, 11, 27), True),   # Friday half day
    (date(2026, 4, 2), True), (date(2026, 4, 3), False),        # Good Friday: Thursday is the week's last session
    (date(2026, 12, 24), True),                                 # Christmas on Friday
    (date(2026, 11, 28), False),                                # Saturday
])
def test_last_session_of_week(day, last):
    assert mc.last_session_of_week(day) is last


def test_upcoming_and_summary_fields():
    assert mc.upcoming(date(2026, 12, 14), 14) == ["12-24 周四 半日市 09:30–13:00（3.5 小时）", "12-25 周五 休市（圣诞节）"]
    assert mc.upcoming(date(2026, 11, 27), 14) == []
    level, fields = calendar_fields(date(2026, 11, 27))
    assert level == "ok" and fields[0][1].startswith("11-27 周五 半日市") and fields[1][1] == "无"


def test_run_writes_the_curated_table(tmp_path):
    path = mc.run(tmp_path)
    assert path == tmp_path / "curated" / "market_calendar.parquet"
    assert pl.read_parquet(path).filter(pl.col("day") == date(2026, 11, 26))["holiday"].item() == "Thanksgiving"
