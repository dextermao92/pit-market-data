"""P4: the same query gives different answers as of different times.

XYZ on 2026-06-09, two 5-min bars (10:00 and 10:05 ET):
  10:00 bar  fetched live 10:05 ET, close 120; refetched by eod 17:30 ET, vendor revised it to close 121
  10:05 bar  fetched live 10:10 ET, close 122
2:1 split, ex 2026-06-10, but we only learn of it on 2026-06-12 (late). ABC is there to be filtered out.
"""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from pit.query import get_bars, get_chain
from pit.storage import TS, write_parquet

# Exercise in progress: delete this line once pit.query is implemented (strict: passing tests then fail CI).
pytestmark = pytest.mark.xfail(raises=NotImplementedError, strict=True)

UTC = timezone.utc
ET_10_00 = datetime(2026, 6, 9, 14, 0, tzinfo=UTC)
ET_10_05 = datetime(2026, 6, 9, 14, 5, tzinfo=UTC)
ET_10_10 = datetime(2026, 6, 9, 14, 10, tzinfo=UTC)
EOD = datetime(2026, 6, 9, 21, 30, tzinfo=UTC)
DAY = (datetime(2026, 6, 9, 13, 30, tzinfo=UTC), datetime(2026, 6, 9, 20, 0, tzinfo=UTC))


def stage(data_dir, dataset, rows, schema=None):
    """Write rows the way the pipeline does: one staged file per fetch (ingested_at)."""
    df = pl.DataFrame(rows, schema_overrides=schema)
    for (at,), part in df.group_by("ingested_at"):
        write_parquet(part, data_dir / "staged" / dataset / f"ingest_date={at:%Y-%m-%d}" / f"{at:%Y%m%dT%H%M%S%fZ}.parquet")


def bar(ingested_at, symbol, bar_start, close):
    return {"ingested_at": ingested_at, "symbol": symbol, "bar_start": bar_start, "open": close, "high": close,
            "low": close, "close": close, "volume": 1000, "trade_count": 10, "vwap": close, "flags": []}


@pytest.fixture
def data(tmp_path):
    stage(tmp_path, "equity_bars", [
        bar(ET_10_05, "XYZ", ET_10_00, 120.0), bar(ET_10_05, "ABC", ET_10_00, 50.0),
        bar(ET_10_10, "XYZ", ET_10_05, 122.0),
        bar(EOD, "XYZ", ET_10_00, 121.0), bar(EOD, "XYZ", ET_10_05, 122.0), bar(EOD, "ABC", ET_10_00, 50.0),
    ], {"ingested_at": TS, "bar_start": TS, "flags": pl.List(pl.String)})
    stage(tmp_path, "corporate_actions", [
        {"ingested_at": datetime(2026, 6, 12, 12, 0, tzinfo=UTC), "id": "s1", "symbol": "XYZ", "type": "forward_split",
         "ex_date": date(2026, 6, 10), "process_date": date(2026, 6, 10), "price_factor": 0.5, "cash": None,
         "special": False, "flags": []},
    ], {"ingested_at": TS, "cash": pl.Float64, "flags": pl.List(pl.String)})
    return tmp_path


def closes(data, as_of, symbols=("XYZ",)):
    out = get_bars(list(symbols), *DAY, as_of, data_dir=data)
    return out["close"].to_list()


def test_nothing_known_before_the_first_fetch(data):
    assert get_bars(["XYZ"], *DAY, datetime(2026, 6, 9, 14, 4, tzinfo=UTC), data_dir=data).height == 0


def test_live_value_then_vendor_revision(data):
    assert closes(data, datetime(2026, 6, 9, 14, 7, tzinfo=UTC)) == [120.0]           # 10:05 bar not fetched yet
    assert closes(data, datetime(2026, 6, 9, 15, 0, tzinfo=UTC)) == [120.0, 122.0]    # live values
    assert closes(data, datetime(2026, 6, 9, 22, 0, tzinfo=UTC)) == [121.0, 122.0]    # after eod: revised


def test_late_split_adjusts_only_once_known(data):
    assert closes(data, datetime(2026, 6, 11, tzinfo=UTC)) == [121.0, 122.0]   # ex_date passed, we didn't know
    out = get_bars(["XYZ"], *DAY, datetime(2026, 6, 13, tzinfo=UTC), data_dir=data)
    assert out["close"].to_list() == pytest.approx([60.5, 61.0])
    assert out["volume"].to_list() == pytest.approx([2000, 2000])


def test_same_as_of_is_reproducible_after_later_fetches(data):
    as_of = datetime(2026, 6, 9, 15, 0, tzinfo=UTC)
    before = get_bars(["XYZ"], *DAY, as_of, data_dir=data)
    stage(data, "equity_bars", [bar(datetime(2026, 6, 15, tzinfo=UTC), "XYZ", ET_10_00, 999.0)],
          {"ingested_at": TS, "bar_start": TS, "flags": pl.List(pl.String)})
    assert get_bars(["XYZ"], *DAY, as_of, data_dir=data).equals(before)


def test_one_row_per_bar_sorted_and_filtered(data):
    out = get_bars(["ABC", "XYZ"], *DAY, datetime(2026, 6, 10, tzinfo=UTC), data_dir=data)
    assert out.select("symbol", "bar_start").rows() == [("ABC", ET_10_00), ("XYZ", ET_10_00), ("XYZ", ET_10_05)]
    assert out.filter(pl.col("symbol") == "XYZ")["ingested_at"].to_list() == [EOD, EOD]   # the version used
    assert get_bars(["XYZ"], ET_10_05, DAY[1], datetime(2026, 6, 10, tzinfo=UTC), data_dir=data).height == 1


@pytest.fixture
def chains(tmp_path):
    def quote(at, underlying, strike):
        return {"ingested_at": at, "underlying": underlying, "trading_class": underlying, "expiry": date(2026, 7, 17),
                "strike": strike, "right": "C", "bid": 1.0, "ask": 1.1, "quote_status": "two_sided"}
    stage(tmp_path, "option_snapshots", [
        quote(ET_10_00, "SPY", 600.0), quote(ET_10_00, "SPY", 605.0),
        quote(ET_10_05, "QQQ", 500.0),
        quote(datetime(2026, 6, 9, 14, 15, tzinfo=UTC), "SPY", 600.0),
    ], {"ingested_at": TS})
    return tmp_path


def test_chain_is_the_latest_snapshot_at_or_before_at(chains):
    out = get_chain("SPY", ET_10_10, data_dir=chains)
    assert out["ingested_at"].unique().to_list() == [ET_10_00] and sorted(out["strike"]) == [600.0, 605.0]
    assert get_chain("SPY", datetime(2026, 6, 9, 13, 59, tzinfo=UTC), data_dir=chains).height == 0


def test_chain_ignores_other_underlyings(chains):
    out = get_chain("QQQ", ET_10_10, data_dir=chains)
    assert out["ingested_at"].unique().to_list() == [ET_10_05] and out["underlying"].unique().to_list() == ["QQQ"]
