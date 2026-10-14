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
