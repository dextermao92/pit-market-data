"""Snapshot option chains from IB Gateway into raw/ — one JSON file per run, every chain in config [[chains]].

Usage: python -m pit.ibkr_options config/ibkr.toml
"""

import json
import math
import os
import shutil
import sys
import time
import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import IB, Contract, Option, StartupFetch

from pit import ibc
from pit.market_hours import calendar
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


def is_monthly(expiry: str) -> bool:
    """Standard monthly for stock/ETF options: the third Friday, or the session before it if that's a holiday."""
    d = date(int(expiry[:4]), int(expiry[4:6]), int(expiry[6:8]))
    first = date(d.year, d.month, 1)
    third_friday = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
    try:
        return d == calendar().date_to_session(str(third_friday), "previous").date()
    except Exception:  # beyond the calendar's horizon (LEAPS): no holiday adjustment
        return d == third_friday


def pick_expiries(expirations, today: date, chain: dict) -> list[str]:
    """dte = [min, max]: every expiry in range (monthly = true: third-Friday expiries only, for underlyings that
    list dailies/weeklies). target_dte = [...]: the nearest expiry to each target."""
    live = sorted(e for e in expirations if _dte(e, today) >= 0)
    if "dte" in chain:
        lo, hi = chain["dte"]
        live = [e for e in live if lo <= _dte(e, today) <= hi]
    if chain.get("monthly"):
        live = [e for e in live if is_monthly(e)]
    if "dte" in chain:
        return live
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
    """Run cadence is 5 min; a chain with every_minutes = 15 is taken on the 5-min slots where
    slot % 15 == offset_minutes (default 0), so heavy chains can be staggered."""
    slot = (now_et.hour * 60 + now_et.minute) // 5 * 5
    return slot % chain.get("every_minutes", 5) == chain.get("offset_minutes", 0)


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


def wait_for_ticks(ib: IB, ib_cfg: dict, tickers, ready, min_seconds: float | None = None) -> None:
    """First-time subscriptions or a data farm still connecting (e.g. right after login) can take >3s:
    wait the minimum (wait_seconds unless given), then until ready(t) holds for every ticker, capped at max_wait_seconds."""
    min_seconds = ib_cfg["wait_seconds"] if min_seconds is None else min_seconds
    ib.sleep(min_seconds)
    deadline = time.monotonic() + ib_cfg["max_wait_seconds"] - min_seconds
    while time.monotonic() < deadline and not all(ready(t) for t in tickers):
        ib.sleep(0.25)


def wait_for_greeks(ib: IB, ib_cfg: dict, tickers) -> None:
    """Model greeks come ~1 s after a contract's quote, later down a batch (2026-11-09 RTH: all 78 by 4.3 s, 12-45 by
    3 s). Read at the quote, 42% of quoted rows had none. A few contracts never get them, hence the cap."""
    deadline = time.monotonic() + ib_cfg.get("greeks_wait_seconds", 0)
    while time.monotonic() < deadline and not all(t.modelGreeks is not None for t in tickers if t.time):
        ib.sleep(0.25)


def spot_of(ib: IB, ib_cfg: dict, und: Contract) -> tuple[float, str | None, dict]:
    """Spot, its tick time, and the quote it came from. Stocks/ETFs wait for both quote sides: until they arrive
    marketPrice() is the last trade, however old (2026-10-30 13:38 AAPL: 347.05 while it traded 346.1-346.7);
    with them it is the last only inside the spread, else the midpoint. Indices have no quote: last or close."""
    t = ib.reqMktData(und)  # streamed, not snapshot: a snapshot of an index outside RTH returns nothing
    has_quote = und.secType != "IND"
    wait_for_ticks(ib, ib_cfg, [t], (lambda t: t.hasBidAsk()) if has_quote  # what marketPrice() checks
                   else lambda t: clean(t.marketPrice()) or clean(t.close))
    ib.cancelMktData(und)
    spot = clean(t.marketPrice()) or clean(t.close)  # indices only calculate in RTH; fall back to close
    if spot is None:
        # Usually the US data farm dropped (seen after overnight server reconnects): ask IBC to reconnect it
        # so the task's retry finds it back, then fail this attempt.
        try:
            reply = ibc.command("RECONNECTDATA")
        except OSError as e:
            reply = f"IBC unreachable: {e!r}"
        raise RuntimeError(f"{und.symbol}: no spot price; sent RECONNECTDATA ({reply}); "
                           "if it persists, check the market-data subscription for the underlying")
    quote = {"spot_bid": clean(t.bid), "spot_ask": clean(t.ask), "spot_last": clean(t.last)}
    return spot, t.time.isoformat() if t.time else None, quote


UNDERLYING_FIELDS = ("conId", "symbol", "secType", "exchange", "currency")
CONTRACT_FIELDS = ("symbol", "lastTradeDateOrContractMonth", "strike", "right", "multiplier", "currency",
                   "conId", "localSymbol", "tradingClass")


def _cache_root() -> Path:
    return Path(os.environ.get("DATA_DIR", "data")) / "cache" / "ibkr_contracts"


def day_cached(name: str, today: date, fetch, refresh: bool = False):
    """JSON from today's cache file, else fetch() and store it. Contract definitions don't change intraday, so a run
    asks IBKR's contract-definition (secdef) server nothing once warm_cache has run: on 2026-10-23 10:35 that server
    stalled ~5 min while quotes kept flowing, and took a whole run with it. Derived and safe to delete."""
    path = _cache_root() / str(today) / name
    if path.exists() and not refresh:
        return json.loads(path.read_text())
    data = fetch()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)
    return data


def underlying(ib: IB, chain: dict, today: date, refresh: bool = False) -> tuple[Contract, list[str]]:
    """The chain's qualified underlying and the expirations listed for its trading class."""
    def fetch():
        und = Contract(symbol=chain["symbol"], secType=chain["sec_type"], exchange=chain["exchange"],
                       currency=chain["currency"])
        ib.qualifyContracts(und)
        params = next(c for c in ib.reqSecDefOptParams(und.symbol, "", und.secType, und.conId)
                      if c.tradingClass == chain["trading_class"] and c.exchange == "SMART")
        return {"underlying": {f: getattr(und, f) for f in UNDERLYING_FIELDS}, "expirations": sorted(params.expirations)}
    d = day_cached(f"{chain['symbol']}_{chain['trading_class']}_params.json", today, fetch, refresh)
    return Contract(**d["underlying"]), d["expirations"]


def listed_contracts(ib: IB, chain: dict, expiry: str, today: date, refresh: bool = False) -> list[Option]:
    """Every contract listed for one expiry (cold: 1-3 min)."""
    def fetch():
        template = Option(chain["symbol"], expiry, exchange="SMART", currency=chain["currency"],
                          tradingClass=chain["trading_class"])
        return [{f: getattr(d.contract, f) for f in CONTRACT_FIELDS} for d in ib.reqContractDetails(template)]
    rows = day_cached(f"{chain['symbol']}_{chain['trading_class']}_{expiry}.json", today, fetch, refresh)
    return [Option(exchange="SMART", **d) for d in rows]


def snapshot_chain(ib: IB, ib_cfg: dict, chain: dict, today: date, spots: dict, taken: set,
                   deadline: float | None = None) -> dict:
    def check_budget():
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError("run budget used up")
    und, expirations = underlying(ib, chain, today)
    if und.conId not in spots:  # one spot per underlying per run, shared by its chains
        spots[und.conId] = spot_of(ib, ib_cfg, und)
    spot, spot_time, spot_quote = spots[und.conId]

    contracts = []
    for exp in pick_expiries(expirations, today, chain):
        check_budget()  # a cold contract-details answer can take minutes
        for c in listed_contracts(ib, chain, exp, today):
            if c.conId not in taken and keep_strike(c.strike, c.right, spot, chain):
                taken.add(c.conId)
                contracts.append(c)

    # Stream a batch, wait, read, cancel: keeps us under the market-data line limit.
    rows, n = [], ib_cfg["batch_size"]
    for i in range(0, len(contracts), n):
        check_budget()
        chunk = contracts[i : i + n]
        tickers = [ib.reqMktData(c, OPEN_INTEREST) for c in chunk]
        wait_for_ticks(ib, ib_cfg, tickers, lambda t: t.time, ib_cfg.get("batch_wait_seconds"))
        wait_for_greeks(ib, ib_cfg, tickers)
        rows +=[ticker_row(t) for t in tickers]
        for c in chunk:
            ib.cancelMktData(c)

    return {"chain": chain["name"], "symbol": chain["symbol"], "trading_class": chain["trading_class"],
            "spot": spot, "spot_time": spot_time, **spot_quote, "contracts": rows}


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
    """Pre-open: bring today's contract cache up to date for every chain (no market data), so the 09:10+ snapshots
    ask the contract-definition (secdef) server nothing.

    The previous day's cache is carried forward first, before even connecting: on 2026-10-27 the secdef server
    answered nothing from 08:15 until a Gateway restart, and a refresh-first warm-up left the day without a cache.
    The refresh is then kept small and paced: re-listing every expiry (~25 MB, ~22k messages) stalled the secdef
    server about five minutes in, every day from 2026-10-28 to 11-02, and only a Gateway restart (2FA) brought it
    back. So: underlyings + expirations (a few small answers), contract lists only for expiries not yet cached, and
    a few of the oldest cached lists (newly listed strikes), `warm_pause_seconds` apart. The refresh stops at its
    first failure (each unanswered request waits RequestTimeout) and fails the task, while runs use the cache."""
    cfg = tomllib.loads(Path(config_path).read_text())
    ib_cfg = cfg["ibkr"]
    t0, today = time.monotonic(), datetime.now(ZoneInfo("America/New_York")).date()
    carried = carry_forward(today)
    pause = ib_cfg.get("warm_pause_seconds", 2)
    ib, errors = connect(ib_cfg, client_offset=8)
    todo, fetched = [], 0
    try:
        for chain in cfg["chains"]:  # underlyings + expirations first (fast), so runs can start from the cache
            _, expirations = underlying(ib, chain, today, refresh=True)
            todo += [(chain, exp) for exp in pick_expiries(expirations, today, chain)]
            ib.sleep(pause)
        for chain, exp in to_refresh(todo, today, ib_cfg.get("warm_relist_per_day", 4)):
            listed_contracts(ib, chain, exp, today, refresh=True)
            fetched += 1
            ib.sleep(pause)
    except Exception as e:
        raise RuntimeError(f"cache refresh stopped after {fetched} contract lists: {e!r}; "
                           f"runs use the {carried} files carried forward") from e
    finally:
        ib.disconnect()
    return {"expiries": len(todo), "listed": fetched, "carried_forward": carried,
            "seconds": round(time.monotonic() - t0)}


def to_refresh(todo: list, today: date, relist: int) -> list:
    """The (chain, expiry) pairs to list from IBKR today: every one without a cached list, then the `relist` whose
    cached lists are oldest (carry_forward keeps the fetch time), to pick up newly listed strikes."""
    def path(chain, exp):
        return _cache_root() / str(today) / f"{chain['symbol']}_{chain['trading_class']}_{exp}.json"
    missing = [(c, e) for c, e in todo if not path(c, e).exists()]
    cached = sorted((path(c, e).stat().st_mtime, i) for i, (c, e) in enumerate(todo) if path(c, e).exists())
    return missing + [todo[i] for _, i in cached[:relist]]


def carry_forward(today: date) -> int:
    """Copy every file of the latest earlier day's cache that today's lacks: underlyings + expirations, index
    contracts (indices.json) and the contract lists of expiries not yet past. Copies keep their modification time,
    which is when IBKR was last asked: warm_cache re-lists the oldest first."""
    root = _cache_root()
    earlier = sorted(d for d in root.glob("*") if d.is_dir() and d.name < str(today))
    if not earlier:
        return 0
    copied = 0
    for src in sorted(earlier[-1].glob("*.json")):
        expiry = src.stem.rsplit("_", 1)[-1]  # <symbol>_<class>_<YYYYMMDD>.json; also _params.json, indices.json
        dst = root / str(today) / src.name
        if (expiry.isdigit() and expiry < f"{today:%Y%m%d}") or dst.exists():
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1
    return copied


def run(config_path: str) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text())
    ingested_at, t0 = datetime.now(timezone.utc), time.monotonic()
    now_et = datetime.now(ZoneInfo("America/New_York"))
    ib, errors = connect(cfg["ibkr"])
    chains, chain_errors = [], []
    try:
        spots, taken = {}, set()
        deadline = t0 + cfg["ibkr"].get("run_budget_seconds", 360)
        for c in (c for c in cfg["chains"] if due(c, now_et)):
            if time.monotonic() > deadline:  # write what we have rather than hit the task timeout
                chain_errors.append({"chain": c["name"], "error": "skipped: run budget used up"})
                continue
            try:  # one bad chain (e.g. a cold contract-details timeout) must not cost the others their snapshot
                chains.append(snapshot_chain(ib, cfg["ibkr"], c, now_et.date(), spots, taken, deadline))
            except Exception as e:
                chain_errors.append({"chain": c["name"], "error": repr(e)[:500]})
    finally:
        ib.disconnect()
    if not chains and chain_errors:
        raise RuntimeError(f"every chain failed: {chain_errors}; IBKR codes: {sorted({e['code'] for e in errors})}")

    payload = {
        "ingested_at": ingested_at.isoformat(),
        "duration_s": round(time.monotonic() - t0, 1),
        "config": cfg,
        "underlyings": chains,  # one entry per chain (key kept from the single-underlying layout)
        "chain_errors": chain_errors,
        "errors": errors,
    }
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "ibkr", "option_snapshots", payload, ingested_at)


if __name__ == "__main__":
    print(run(sys.argv[1]))
