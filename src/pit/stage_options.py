"""Raw IBKR option snapshot JSON -> typed staged Parquet, one file per raw file.

Staged is a pure function of raw: re-staging gives identical data.
Usage (re-stage everything): python -m pit.stage_options data
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

from pit.storage import TS, staged_path, write_parquet

QUOTE = {  # raw field -> staged column; IBKR's -1 means "no value" and becomes null
    "bid": "bid", "bidSize": "bid_size", "ask": "ask", "askSize": "ask_size",
    "last": "last", "lastSize": "last_size", "volume": "volume", "close": "close",
}
GREEKS = {
    "impliedVol": "iv", "delta": "delta", "gamma": "gamma", "vega": "vega", "theta": "theta",
    "optPrice": "model_price", "pvDividend": "pv_dividend", "undPrice": "model_und_price",
}
SCHEMA = {
    "ingested_at": TS, "underlying": pl.String, "trading_class": pl.String, "spot": pl.Float64, "spot_time": TS,
    "con_id": pl.Int64, "local_symbol": pl.String, "expiry": pl.Date, "strike": pl.Float64, "right": pl.String,
    "multiplier": pl.Int32,
    **{c: pl.Float64 for c in QUOTE.values()},
    "open_interest": pl.Float64, "market_data_type": pl.Int8, "tick_time": TS,
    **{c: pl.Float64 for c in GREEKS.values()},
}


def _ts(s):
    return datetime.fromisoformat(s) if s else None


def _value(x):
    return None if x == -1 else x


def to_frame(payload: dict) -> pl.DataFrame:
    rows = []
    for u in payload["underlyings"]:
        for c in u["contracts"]:
            g = c["modelGreeks"] or {}
            rows.append({
                "ingested_at": _ts(payload["ingested_at"]),
                "underlying": u["symbol"],
                "trading_class": c["tradingClass"],
                "spot": u["spot"],
                "spot_time": _ts(u["spot_time"]),
                "con_id": c["conId"],
                "local_symbol": c["localSymbol"],
                "expiry": datetime.strptime(c["expiry"], "%Y%m%d").date(),
                "strike": c["strike"],
                "right": c["right"],
                "multiplier": int(c["multiplier"]) if c["multiplier"] else None,
                **{col: _value(c[f]) for f, col in QUOTE.items()},
                # IBKR puts an option's OI in the field matching its right; the other one is 0.
                "open_interest": c["callOpenInterest"] if c["right"] == "C" else c["putOpenInterest"],
                "market_data_type": c["marketDataType"],
                "tick_time": _ts(c["tick_time"]),
                **{col: g.get(f) for f, col in GREEKS.items()},
            })
    bid, ask = pl.col("bid").fill_null(0), pl.col("ask").fill_null(0)
    quote_status = (
        pl.when((bid > 0) & (ask > 0) & (bid > ask)).then(pl.lit("crossed"))
        .when((bid > 0) & (ask > 0)).then(pl.lit("two_sided"))
        .when(ask > 0).then(pl.lit("ask_only"))
        .when(bid > 0).then(pl.lit("bid_only"))
        .otherwise(pl.lit("no_quote"))
    )
    return pl.DataFrame(rows, schema=SCHEMA).with_columns(quote_status=quote_status)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame]:
    df = to_frame(json.loads(raw_path.read_text()))
    return write_parquet(df, staged_path(raw_path)), df


if __name__ == "__main__":
    for raw in sorted(Path(sys.argv[1]).glob("raw/source=ibkr/ingest_date=*/option_snapshots/*.json")):
        print(stage_file(raw)[0])
