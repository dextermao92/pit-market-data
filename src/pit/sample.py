"""Synthetic sample for CI: raw files for every dataset, staged by the real staging code.

Usage: python -m pit.sample sample_data   then   DATA_DIR=sample_data dbt build --project-dir dbt --profiles-dir dbt
Small and deterministic; it only has to be internally consistent (5-min bars fit the daily bar, the option
snapshot's spot fits its bar), so the dbt tests pass and mean something.
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pit import alpaca_bars, assets, corporate_actions, market_calendar, stage_options
from pit.storage import write_raw

UTC = timezone.utc
OPEN = datetime(2026, 10, 14, 13, 30, tzinfo=UTC)  # 09:30 ET
FETCHED = datetime(2026, 10, 14, 21, 0, tzinfo=UTC)
BASE = {"SPY": 700.0, "AAPL": 250.0}


def _bars(symbol: str) -> list[dict]:
    out = []
    for i in range(78):
        c = BASE[symbol] * (1 + 0.002 * math.sin(i / 7))
        o = BASE[symbol] * (1 + 0.002 * math.sin((i - 1) / 7))
        out.append({"t": (OPEN + timedelta(minutes=5 * i)).isoformat().replace("+00:00", "Z"), "o": o,
                    "h": max(o, c) + 0.05, "l": min(o, c) - 0.05, "c": c, "v": 10_000 + 100 * i, "n": 50, "vw": c})
    return out


def build(data_dir: Path) -> None:
    bars = {s: _bars(s) for s in BASE}
    prev = {s: [b[-1] | {"t": "2026-10-13T19:55:00Z"}] for s, b in bars.items()}
    stamp = FETCHED.isoformat()

    request = {"symbols": ",".join(BASE), "timeframe": "5Min", "start": OPEN.isoformat(),
               "end": (OPEN + timedelta(minutes=5 * 77)).isoformat()}
    raw = write_raw(data_dir, "alpaca", "equity_bars", {"ingested_at": stamp, "duration_s": 1.0, "session": "2026-10-14",
                    "request": request, "pages": [{"bars": bars}], "prev_pages": [{"bars": prev}]}, FETCHED)
    alpaca_bars.stage_file(raw)

    daily = {s: [{"t": "2026-10-14T04:00:00Z", "o": b[0]["o"], "h": max(x["h"] for x in b), "l": min(x["l"] for x in b),
                  "c": b[-1]["c"], "v": int(sum(x["v"] for x in b) * 1.25), "n": 5000, "vw": b[-1]["c"]}]
             for s, b in bars.items()}  # vendor daily bars include extended hours: more volume
    raw = write_raw(data_dir, "alpaca", "daily_bars",
                    {"ingested_at": stamp, "duration_s": 1.0, "request": {}, "pages": [{"bars": daily}]}, FETCHED)
    alpaca_bars.stage_file(raw)

    snap_at = OPEN + timedelta(hours=1, seconds=30)
    spot = bars["SPY"][12]["c"]  # the 10:30 bar contains the snapshot time
    contracts = [{"conId": 1000 + 10 * k + (r == "P"), "localSymbol": f"SPY 261016{r}{int(spot) + k}", "tradingClass": "SPY",
                  "expiry": "20261016", "strike": float(int(spot) + k), "right": r, "multiplier": "100",
                  "bid": 2.0, "bidSize": 10.0, "ask": 2.1, "askSize": 10.0, "last": 2.05, "lastSize": 1.0,
                  "volume": 100.0, "close": 2.0, "callOpenInterest": 500.0 if r == "C" else 0.0,
                  "putOpenInterest": 500.0 if r == "P" else 0.0, "marketDataType": 1, "tick_time": snap_at.isoformat(),
                  "modelGreeks": {"impliedVol": 0.15, "delta": 0.5 if r == "C" else -0.5, "gamma": 0.01, "vega": 0.3,
                                  "theta": -0.2, "optPrice": 2.05, "pvDividend": 0.0, "undPrice": spot}}
                 for k in (-1, 0, 1) for r in ("C", "P")]
    raw = write_raw(data_dir, "ibkr", "option_snapshots", {
        "ingested_at": snap_at.isoformat(), "duration_s": 1.0, "config": {}, "errors": [],
        "underlyings": [{"symbol": "SPY", "trading_class": "SPY", "spot": spot, "spot_time": snap_at.isoformat(),
                         "contracts": contracts}]}, snap_at)
    stage_options.stage_file(raw)

    raw = write_raw(data_dir, "alpaca", "corporate_actions", {"ingested_at": stamp, "duration_s": 1.0, "request": {},
        "pages": [{"corporate_actions": {
            "forward_splits": [{"id": "s1", "symbol": "AAPL", "old_rate": 1, "new_rate": 4, "ex_date": "2020-08-31",
                                "process_date": "2020-08-31"}],
            "cash_dividends": [{"id": "d1", "symbol": "SPY", "rate": 1.9, "special": False, "ex_date": "2026-09-18",
                                "process_date": "2026-10-30"}]}}]}, FETCHED)
    corporate_actions.stage_file(raw)

    for day, aapl_name in ((1, "Apple Inc."), (2, "Apple Inc. Common Stock")):  # a rename -> two SCD2 versions
        at = FETCHED + timedelta(days=day)
        raw = write_raw(data_dir, "alpaca", "assets", {"ingested_at": at.isoformat(), "duration_s": 1.0, "missing": [],
            "assets": [{"id": "a-spy", "symbol": "SPY", "name": "SPDR S&P 500", "exchange": "ARCA", "class": "us_equity",
                        "status": "active", "tradable": True},
                       {"id": "a-aapl", "symbol": "AAPL", "name": aapl_name, "exchange": "NASDAQ", "class": "us_equity",
                        "status": "active", "tradable": True}]}, at)
        assets.stage_file(raw)
    market_calendar.run(data_dir)  # derived from exchange_calendars, not fetched: the real table


if __name__ == "__main__":
    build(Path(sys.argv[1]))
