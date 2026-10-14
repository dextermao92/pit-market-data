"""Snapshot option chains from IB Gateway into raw/ — one JSON file per run.

Usage: python -m pit.ibkr_options config/ibkr_options.toml
"""

import math
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import IB, Contract, Option, StartupFetch

from pit.storage import write_raw

TICKER_FIELDS = (
    "bid", "bidSize", "ask", "askSize", "last", "lastSize", "volume", "close",
    "callOpenInterest", "putOpenInterest", "marketDataType",
)
GREEK_FIELDS = ("impliedVol", "delta", "gamma", "vega", "theta", "optPrice", "pvDividend", "undPrice")
OPEN_INTEREST = "101"  # generic tick list; not allowed with snapshot=True, hence stream-then-cancel


def clean(x):
    """NaN -> None (JSON has no NaN; ib_async uses NaN for an empty quote side). Everything else kept as received."""
    return None if isinstance(x, float) and math.isnan(x) else x


def pick_expiries(expirations, today: str, n: int) -> list[str]:
    return sorted(e for e in expirations if e >= today)[:n]


def ticker_row(t) -> dict:
    c, g = t.contract, t.modelGreeks
    return {
        "conId": c.conId,
        "localSymbol": c.localSymbol,
        "tradingClass": c.tradingClass,
        "expiry": c.lastTradeDateOrContractMonth,
        "strike": c.strike,
        "right": c.right,
        "multiplier": c.multiplier,
        **{f: clean(getattr(t, f)) for f in TICKER_FIELDS},
        "tick_time": t.time.isoformat() if t.time else None,
        "modelGreeks": {f: clean(getattr(g, f)) for f in GREEK_FIELDS} if g else None,
    }


def snapshot_underlying(ib: IB, ib_cfg: dict, u: dict, today: str) -> dict:
    und = Contract(symbol=u["symbol"], secType=u["sec_type"], exchange=u["exchange"], currency=u["currency"])
    ib.qualifyContracts(und)
    und_t = ib.reqMktData(und)  # streamed, not snapshot: a snapshot of an index outside RTH returns nothing
    ib.sleep(ib_cfg["wait_seconds"])
    ib.cancelMktData(und)
    spot = clean(und_t.marketPrice()) or clean(und_t.close)  # indices only calculate in RTH; fall back to close
    if spot is None:
        raise RuntimeError(f"{u['symbol']}: no spot price; check the market-data subscription for the underlying")

    chain = next(
        c for c in ib.reqSecDefOptParams(und.symbol, "", und.secType, und.conId)
        if c.tradingClass == u["trading_class"] and c.exchange == "SMART"
    )
    # One contract-details request per expiry returns the strikes that really exist for it.
    contracts = []
    for exp in pick_expiries(chain.expirations, today, u["max_expiries"]):
        template = Option(u["symbol"], exp, exchange="SMART", currency=u["currency"], tradingClass=u["trading_class"])
        contracts += [
            d.contract for d in ib.reqContractDetails(template)
            if abs(d.contract.strike - spot) <= spot * u["strike_range_pct"]
        ]

    # Stream a batch, wait, read, cancel: keeps us under the market-data line limit.
    rows, n = [], ib_cfg["batch_size"]
    for i in range(0, len(contracts), n):
        chunk = contracts[i : i + n]
        tickers = [ib.reqMktData(c, OPEN_INTEREST) for c in chunk]
        # Contracts subscribed for the first time (or a waking data farm) can take >3s to tick:
        # wait the minimum, then until every ticker has ticked, capped at max_wait_seconds.
        ib.sleep(ib_cfg["wait_seconds"])
        deadline = time.monotonic() + ib_cfg["max_wait_seconds"] - ib_cfg["wait_seconds"]
        while time.monotonic() < deadline and not all(t.time for t in tickers):
            ib.sleep(0.25)
        rows += [ticker_row(t) for t in tickers]
        for c in chunk:
            ib.cancelMktData(c)

    return {
        "symbol": u["symbol"],
        "trading_class": u["trading_class"],
        "spot": spot,
        "spot_time": und_t.time.isoformat() if und_t.time else None,
        "contracts": rows,
    }


def run(config_path: str) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text())
    ib_cfg = cfg["ibkr"]
    ingested_at, t0 = datetime.now(timezone.utc), time.monotonic()
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")

    errors = []
    ib = IB()
    ib.errorEvent += lambda req_id, code, msg, contract: errors.append(
        {"reqId": req_id, "code": code, "msg": msg, "conId": getattr(contract, "conId", None)}
    )
    ib.connect(
        os.environ.get("IB_HOST", ib_cfg["host"]),
        int(os.environ.get("IB_PORT", ib_cfg["port"])),
        clientId=ib_cfg["client_id"],
        readonly=True,
        fetchFields=StartupFetch(0),  # don't pull positions/orders/account into a data pipeline
    )
    try:
        ib.reqMarketDataType(ib_cfg["market_data_type"])
        underlyings = [snapshot_underlying(ib, ib_cfg, u, today) for u in cfg["underlyings"]]
    finally:
        ib.disconnect()

    payload = {
        "ingested_at": ingested_at.isoformat(),
        "duration_s": round(time.monotonic() - t0, 1),
        "config": cfg,
        "underlyings": underlyings,
        "errors": errors,
    }
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "ibkr", "option_snapshots", payload, ingested_at)


if __name__ == "__main__":
    print(run(sys.argv[1]))
