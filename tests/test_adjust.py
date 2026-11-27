"""Golden test: adjustment factors computed by hand.

XYZ, one bar per session (unadjusted close, volume 1000):
  A 2026-03-02  close 100
  B 2026-03-03  close  99   <- $1.00 cash dividend, ex 2026-03-03: factor 1 - 1/100 = 0.99 for A
  C 2026-06-09  close 120
  D 2026-06-10  close  60   <- 2:1 split, ex 2026-06-10: factor 0.5 for A, B, C
The split is first ingested on 2026-05-30 and corrected to 3:1 (same id) on 2026-06-20.
"""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from pit.adjust import adjust, session_closes
from pit.storage import TS

UTC = timezone.utc
BARS = pl.DataFrame({
    "symbol": ["XYZ"] * 4,
    "bar_start": [datetime(2026, m, d, 15, 0, tzinfo=UTC) for m, d in ((3, 2), (3, 3), (6, 9), (6, 10))],
    "open": [100.0, 99.0, 120.0, 60.0], "high": [100.0, 99.0, 120.0, 60.0], "low": [100.0, 99.0, 120.0, 60.0],
    "close": [100.0, 99.0, 120.0, 60.0], "vwap": [100.0, 99.0, 120.0, 60.0], "volume": [1000] * 4,
})
ACTIONS = pl.DataFrame({
    "ingested_at": [datetime(2026, 2, 20, tzinfo=UTC), datetime(2026, 5, 30, tzinfo=UTC), datetime(2026, 6, 20, tzinfo=UTC)],
    "id": ["div", "split", "split"], "symbol": ["XYZ"] * 3,
    "ex_date": [date(2026, 3, 3), date(2026, 6, 10), date(2026, 6, 10)],
    "price_factor": [None, 0.5, 1 / 3], "cash": [1.0, None, None],
}, schema_overrides={"ingested_at": TS})


def closes(as_of, how="all"):
    out = adjust(BARS, ACTIONS, session_closes(BARS), datetime(*as_of, tzinfo=UTC), how)
    return out["close"].to_list(), out["volume"].to_list()


@pytest.mark.parametrize("as_of, expected_close, expected_volume", [
    ((2026, 2, 25), [100, 99, 120, 60], [1000] * 4),            # dividend known, not yet effective
    ((2026, 5, 1), [99, 99, 120, 60], [1000] * 4),              # dividend only
    ((2026, 6, 5), [99, 99, 120, 60], [1000] * 4),              # split known but ex_date still ahead
    ((2026, 6, 15), [49.5, 49.5, 60, 60], [2000, 2000, 2000, 1000]),   # 2:1 as first reported
    ((2026, 7, 1), [33, 33, 40, 60], [3000, 3000, 3000, 1000]),        # corrected to 3:1
])
def test_point_in_time_adjustment(as_of, expected_close, expected_volume):
    c, v = closes(as_of)
    assert c == pytest.approx(expected_close) and v == pytest.approx(expected_volume)


def test_split_only():
    assert closes((2026, 6, 15), how="split")[0] == pytest.approx([50, 49.5, 60, 60])


# --- by security (identity's security_days) ---

from pit.adjust import adjust_daily, attach, split_jumps  # noqa: E402

SYMBOLS = pl.DataFrame({
    "security_id": ["meta", "meta", "fund"], "symbol": ["FB", "META", "FB"],
    "valid_from": [date(2016, 1, 4), date(2022, 6, 9), date(2025, 6, 26)],
    "valid_to": [date(2022, 6, 8), date(2026, 11, 20), date(2026, 11, 20)],
})


def day_rows(sec, rows):
    return [{"security_id": sec, "session": d, "symbol": s, "open": c, "high": c, "low": c, "close": c, "vwap": c,
             "volume": v, "no_trade": v == 0} for d, s, c, v in rows]


DAYS = pl.DataFrame(
    day_rows("meta", [(date(2021, 3, 1), "FB", 100.0, 1000), (date(2021, 3, 2), "FB", 100.0, 1000),
                      (date(2023, 3, 1), "META", 50.0, 1000), (date(2023, 3, 2), "META", 50.0, 1000)])
    + day_rows("fund", [(date(2025, 7, 1), "FB", 20.0, 500), (date(2025, 7, 2), "FB", 19.0, 500)]))
SEC_ACTIONS = pl.DataFrame({
    "ingested_at": [datetime(2026, 1, 1, tzinfo=UTC)] * 3, "id": ["split", "div-fb", "div-fund"],
    "symbol": ["META", "FB", "FB"], "ex_date": [date(2023, 3, 2), date(2021, 3, 2), date(2025, 7, 2)],
    "price_factor": [0.5, None, None], "cash": [None, 1.0, 1.0],
}, schema_overrides={"ingested_at": TS})


def test_attach_by_symbol_and_date():
    a = attach(SEC_ACTIONS, SYMBOLS)
    assert dict(zip(a["id"], a["security_id"])) == {"split": "meta", "div-fb": "meta", "div-fund": "fund"}
    lost = attach(SEC_ACTIONS.with_columns(symbol=pl.lit("OTCX")), SYMBOLS)
    assert lost["security_id"].is_null().all()


def test_adjust_daily_follows_the_security_across_a_rename_and_not_a_reused_ticker():
    out = adjust_daily(DAYS, attach(SEC_ACTIONS, SYMBOLS), datetime(2026, 1, 2, tzinfo=UTC)).sort("security_id", "session")
    meta = out.filter(pl.col("security_id") == "meta")
    # META's 2023 split reaches back to the FB-era bars; FB's 2021 dividend (1/100) only to the day before it
    assert meta["close"].to_list() == pytest.approx([100 * 0.5 * 0.99, 50.0, 25.0, 50.0])
    assert meta["volume"].to_list() == pytest.approx([2000, 2000, 2000, 1000])
    fund = out.filter(pl.col("security_id") == "fund")
    assert fund["close"].to_list() == pytest.approx([20 * 0.95, 19.0])   # its own dividend only


def test_split_jumps_flags_moves_without_a_split():
    acts = attach(SEC_ACTIONS, SYMBOLS)
    assert split_jumps(DAYS, acts).height == 0        # FB->META halving is the attached split (2023-03-02)
    no_split = split_jumps(DAYS, acts.filter(pl.col("id") != "split"))
    assert no_split.select("security_id", "session", "split_like").rows() == [("meta", date(2023, 3, 1), True)]
