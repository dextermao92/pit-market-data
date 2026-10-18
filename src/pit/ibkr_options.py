"""Snapshot option chains from IB Gateway into raw/ — one JSON file per run, every chain in config [[chains]].

Usage: python -m pit.ibkr_options config/ibkr.toml
"""

import json
import math
import os
import sys
import time
import tomllib
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import IB, Contract, Option, StartupFetch

from pit import ibc
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


def _dte(expiry: str, today: date) -> int:
    return (date(int(expiry[:4]), int(expiry[4:6]), int(expiry[6:8])) - today).days


def pick_expiries(expirations, today: date, chain: dict) -> list[str]:
    """dte = [min, max]: every expiry in range. target_dte = [...]: the nearest expiry to each target."""
    live = sorted(e for e in expirations if _dte(e, today) >= 0)
    if "dte" in chain:
        lo, hi = chain["dte"]
        return [e for e in live if lo <= _dte(e, today) <= hi]
    return sorted({min(live, key=lambda e: abs(_dte(e, today) - t)) for t in chain["target_dte"]})


def keep_strike(strike: float, right: str, spot: float, chain: dict) -> bool:
    m = strike / spot - 1
    if chain["rights"] == "otm" and ((right == "P" and strike > spot) or (right == "C" and strike < spot)):
        return False
    if m < -chain["put_range"] or m > chain["call_range"]:
        return False
    step = next(step for band, step in chain["strike_steps"] if abs(m) <= band)
    return round(strike * 100) % round(step * 100) == 0


def due(chain: dict, now_et: datetime) -> bool:
    """Run cadence is 5 min; a chain with every_minutes = 15 is taken on the 5-min slots divisible by 15."""
    slot = (now_et.hour * 60 + now_et.minute) // 5 * 5
    return slot % chain.get("every_minutes", 5) == 0


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


def wait_for_ticks(ib: IB, ib_cfg: dict, tickers, ready) -> None:
    """First-time subscriptions or a data farm still connecting (e.g. right after login) can take >3s:
    wait the minimum, then until ready(t) holds for every ticker, capped at max_wait_seconds."""
    ib.sleep(ib_cfg["wait_seconds"])
    deadline = time.monotonic() + ib_cfg["max_wait_seconds"] - ib_cfg["wait_seconds"]
    while time.monotonic() < deadline and not all(ready(t) for t in tickers):
        ib.sleep(0.25)


def spot_of(ib: IB, ib_cfg: dict, chain: dict) -> tuple[Contract, float, str | None]:
    und = Contract(symbol=chain["symbol"], secType=chain["sec_type"], exchange=chain["exchange"],
                   currency=chain["currency"])
    ib.qualifyContracts(und)
    t = ib.reqMktData(und)  # streamed, not snapshot: a snapshot of an index outside RTH returns nothing
    wait_for_ticks(ib, ib_cfg, [t], lambda t: clean(t.marketPrice()) or clean(t.close))
    ib.cancelMktData(und)
    spot = clean(t.marketPrice()) or clean(t.close)  # indices only calculate in RTH; fall back to close
    if spot is None:
        # Usually the US data farm dropped (seen after overnight server reconnects): ask IBC to reconnect it
        # so the task's retry finds it back, then fail this attempt.
        try:
            reply = ibc.command("RECONNECTDATA")
        except OSError as e:
            reply = f"IBC unreachable: {e!r}"
        raise RuntimeError(f"{chain['symbol']}: no spot price; sent RECONNECTDATA ({reply}); "
                           "if it persists, check the market-data subscription for the underlying")
    return und, spot, t.time.isoformat() if t.time else None


CONTRACT_FIELDS = ("symbol", "lastTradeDateOrContractMonth", "strike", "right", "multiplier", "currency",
                   "conId", "localSymbol", "tradingClass")


def _cache_root() -> Path:
    return Path(os.environ.get("DATA_DIR", "data")) / "cache" / "ibkr_contracts"


def listed_contracts(ib: IB, chain: dict, expiry: str, today: date, refresh: bool = False) -> list[Option]:
    """Every contract listed for one expiry. Specs don't change intraday, so the contract-details answer
    (~1 min per run across ~15 expiries) is cached on disk per New York day. Derived and safe to delete."""
    path = _cache_root() / str(today) / f"{chain['symbol']}_{chain['trading_class']}_{expiry}.json"
    if path.exists() and not refresh:
        return [Option(exchange="SMART", **d) for d in json.loads(path.read_text())]
    template = Option(chain["symbol"], expiry, exchange="SMART", currency=chain["currency"],
                      tradingClass=chain["trading_class"])
    contracts = [d.contract for d in ib.reqContractDetails(template)]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps([{f: getattr(c, f) for f in CONTRACT_FIELDS} for c in contracts]))
    tmp.replace(path)
    return [Option(exchange="SMART", **{f: getattr(c, f) for f in CONTRACT_FIELDS}) for c in contracts]


def snapshot_chain(ib: IB, ib_cfg: dict, chain: dict, today: date, spots: dict, taken: set) -> dict:
    key = (chain["symbol"], chain["sec_type"], chain["exchange"])
    if key not in spots:  # one spot per underlying per run, shared by its chains
        spots[key] = spot_of(ib, ib_cfg, chain)
    und, spot, spot_time = spots[key]

    params = next(
        c for c in ib.reqSecDefOptParams(und.symbol, "", und.secType, und.conId)
        if c.tradingClass == chain["trading_class"] and c.exchange == "SMART"
    )
    contracts = []
    for exp in pick_expiries(params.expirations, today, chain):
        for c in listed_contracts(ib, chain, exp, today):
            if c.conId not in taken and keep_strike(c.strike, c.right, spot, chain):
                taken.add(c.conId)
                contracts.append(c)

    # Stream a batch, wait, read, cancel: keeps us under the market-data line limit.
    rows, n = [], ib_cfg["batch_size"]
    for i in range(0, len(contracts), n):
        chunk = contracts[i : i + n]
        tickers = [ib.reqMktData(c, OPEN_INTEREST) for c in chunk]
        wait_for_ticks(ib, ib_cfg, tickers, lambda t: t.time)
        rows += [ticker_row(t) for t in tickers]
        for c in chunk:
            ib.cancelMktData(c)

    return {"chain": chain["name"], "symbol": chain["symbol"], "trading_class": chain["trading_class"],
            "spot": spot, "spot_time": spot_time, "contracts": rows}


def connect(ib_cfg: dict, client_offset: int = 0) -> tuple[IB, list]:
    errors = []
    ib = IB()
    ib.errorEvent += lambda req_id, code, msg, contract: errors.append(
        {"reqId": req_id, "code": code, "msg": msg, "conId": getattr(contract, "conId", None)}
    )
    ib.connect(
        os.environ.get("IB_HOST", ib_cfg["host"]),
        int(os.environ.get("IB_PORT", ib_cfg["port"])),
        clientId=ib_cfg["client_id"] + client_offset,
        readonly=True,
        fetchFields=StartupFetch(0),  # don't pull positions/orders/account into a data pipeline
    )
    # ib_async waits forever by default; after "1100 connectivity lost" a pending request then hangs the whole run
    # (2026-10-16: 26 min). Fail it instead, so the task records the failure and retries. Above the slowest cold
    # contract-details answer seen (208 s).
    ib.RequestTimeout = ib_cfg.get("request_timeout", 300)
    ib.reqMarketDataType(ib_cfg["market_data_type"])
    return ib, errors


def warm_cache(config_path: str) -> dict:
    """Pre-open: fill today's contract cache for every chain (no market data). Cold answers take 1-3 min per
    expiry, so doing this at 08:15 keeps the 09:10+ snapshots fast."""
    cfg = tomllib.loads(Path(config_path).read_text())
    t0, today = time.monotonic(), datetime.now(ZoneInfo("America/New_York")).date()
    ib, errors = connect(cfg["ibkr"], client_offset=8)
    todo, carried = [], 0
    try:
        for chain in cfg["chains"]:
            und = Contract(symbol=chain["symbol"], secType=chain["sec_type"], exchange=chain["exchange"],
                           currency=chain["currency"])
            ib.qualifyContracts(und)
            params = next(c for c in ib.reqSecDefOptParams(und.symbol, "", und.secType, und.conId)
                          if c.tradingClass == chain["trading_class"] and c.exchange == "SMART")
            todo += [(chain, exp) for exp in pick_expiries(params.expirations, today, chain)]
        carried = carry_forward(todo, today)  # usable at once, even if IBKR answers slowly today
        for chain, exp in todo:  # then refresh each from IBKR (newly listed strikes)
            listed_contracts(ib, chain, exp, today, refresh=True)
    finally:
        ib.disconnect()
    return {"expiries": len(todo), "carried_forward": carried, "seconds": round(time.monotonic() - t0)}


def carry_forward(todo: list, today: date) -> int:
    """Copy each expiry's contract list from the latest earlier day's cache into today's, if today lacks it."""
    root = _cache_root()
    earlier = sorted(d for d in root.glob("*") if d.is_dir() and d.name < str(today))
    if not earlier:
        return 0
    copied = 0
    for chain, exp in todo:
        name = f"{chain['symbol']}_{chain['trading_class']}_{exp}.json"
        src, dst = earlier[-1] / name, root / str(today) / name
        if src.exists() and not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
            copied += 1
    return copied


def run(config_path: str) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text())
    ingested_at, t0 = datetime.now(timezone.utc), time.monotonic()
    now_et = datetime.now(ZoneInfo("America/New_York"))
    ib, errors = connect(cfg["ibkr"])
    try:
        spots, taken = {}, set()
        chains = [snapshot_chain(ib, cfg["ibkr"], c, now_et.date(), spots, taken)
                  for c in cfg["chains"] if due(c, now_et)]
    finally:
        ib.disconnect()

    payload = {
        "ingested_at": ingested_at.isoformat(),
        "duration_s": round(time.monotonic() - t0, 1),
        "config": cfg,
        "underlyings": chains,  # one entry per chain (key kept from the single-underlying layout)
        "errors": errors,
    }
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "ibkr", "option_snapshots", payload, ingested_at)


if __name__ == "__main__":
    print(run(sys.argv[1]))
