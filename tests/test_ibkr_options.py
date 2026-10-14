import json
import math
from datetime import datetime, timezone

from ib_async import Option, Ticker

from pit.ibkr_options import pick_expiries, ticker_row
from pit.storage import write_raw


def test_pick_expiries_skips_past_and_sorts():
    assert pick_expiries(["20261016", "20261012", "20261013", "20261014"], "20261013", 2) == ["20261013", "20261014"]


def test_ticker_row_ask_only_quote():
    # ib_async turns an empty side (size 0) into NaN; deep OTM typically looks like this.
    t = Ticker(contract=Option("SPX", "20261013", 7500, "C", tradingClass="SPXW"))
    t.bid, t.ask, t.askSize = math.nan, 0.05, 10.0
    row = ticker_row(t)
    assert row["bid"] is None and row["ask"] == 0.05 and row["last"] is None
    assert row["modelGreeks"] is None
    json.dumps(row)  # must be JSON-serializable


def test_write_raw_is_partitioned_and_complete(tmp_path):
    at = datetime(2026, 10, 13, 14, 30, tzinfo=timezone.utc)
    path = write_raw(tmp_path, "ibkr", "option_snapshots", {"x": 1}, at)
    assert path.parent == tmp_path / "raw/source=ibkr/ingest_date=2026-10-13/option_snapshots"
    assert json.loads(path.read_text()) == {"x": 1}
    assert not list(path.parent.glob("*.tmp"))
