"""Whole-market symbol discovery and daily bars (Stage 2 P1a; design in docs/stage2/p1-universe-design.md).

Alpaca has no list of delisted securities, but still serves their bars. So the candidate symbols are discovered:
every 1..max_symbol_letters letter symbol, plus every non-OTC symbol in the asset list, every symbol named in
corporate actions and the configured symbols, probed with monthly bars since 2016 (cheap: cost follows the real
rows, not the symbols tried). Each probe batch is a raw file (dataset universe_probe, never staged) that lists
every symbol tried, including those that returned nothing, so coverage can be checked later. The candidate list
in cache/ is derived from them and safe to delete; the backfill markers there make reruns incremental
(deleting them means the next backfill fetches everything again).

Acquire only: no deduplication, identity or universe rules here. Back-mapped duplicates (META also returns FB's
history) and reused tickers (BBBY is two companies) land in staged daily_bars as served; P1b curates them.

Scheduled as the universe_refresh DAG (quarterly, Saturday 20:00 ET: US night, Australian daytime). By hand:
  python -m pit.universe refresh config/alpaca_bars.toml          # discover + backfill to the last session
  python -m pit.universe discover config/alpaca_bars.toml
  python -m pit.universe backfill config/alpaca_bars.toml 2016-01-01 2026-11-24
"""

import hashlib
import itertools
import json
import os
import random
import re
import sys
import time
import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from pit import pipeline
from pit.alpaca_bars import client, fetch_pages
from pit.corporate_actions import HISTORY_START
from pit.market_hours import calendar, collection_window
from pit.storage import write_parquet, write_raw

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
ACTION_SYMBOL_KEYS = ("symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol", "source_symbol")
VALID_SYMBOL = re.compile(r"^[A-Z][A-Z0-9]{0,5}([./-][A-Z0-9]{1,4})?$")  # letters, digits, one class suffix
CANDIDATES = Path("cache") / "universe" / "candidates.parquet"
BACKFILL_DONE = Path("cache") / "universe" / "backfill_done"
SESSIONS_PER_MONTH = 21
NY = ZoneInfo("America/New_York")
STOP_MARGIN = timedelta(minutes=5)          # no new batch this close to the collection window
DISCOVER_MIN_TIME = timedelta(minutes=45)   # discovery (~25 min + rechecks) isn't resumable: start only with this


def _now() -> datetime:
    return datetime.now(timezone.utc)


def default_stop_by() -> datetime:
    """When a heavy job must be done: the start of the current or next collection window (08:00 ET on a session
    day). Already past, inside the window, so nothing starts."""
    return collection_window(_now())[0]


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "data"))


def _config(config_path: str) -> dict:
    return tomllib.loads(Path(config_path).read_text(encoding="utf-8"))


def _raw_files(data_dir: Path, dataset: str) -> list[Path]:
    return sorted(data_dir.glob(f"raw/source=alpaca/ingest_date=*/{dataset}/*.json"))  # chronological


def brute_force_symbols(max_letters: int) -> list[str]:
    return ["".join(c) for n in range(1, max_letters + 1) for c in itertools.product(LETTERS, repeat=n)]


def asset_symbols(data_dir: Path, active_only: bool = False) -> set[str]:
    """Non-OTC symbols in the latest assets snapshot. OTC names have no SIP bars."""
    files = _raw_files(data_dir, "assets")
    if not files:
        return set()
    assets = json.loads(files[-1].read_text(encoding="utf-8"))["assets"]
    return {a["symbol"] for a in assets if a.get("symbol") and a.get("exchange") != "OTC"
            and (not active_only or a.get("status") == "active")}


def action_symbols(data_dir: Path) -> set[str]:
    """Every symbol named in corporate actions: the latest full-history fetch and the window fetches after it
    (all files when there is no full fetch yet)."""
    files = _raw_files(data_dir, "corporate_actions")
    payloads = [json.loads(f.read_text(encoding="utf-8")) for f in files]
    full = [i for i, p in enumerate(payloads) if p.get("scope") == "full"]
    out = set()
    for p in payloads[full[-1]:] if full else payloads:
        for page in p["pages"]:
            for actions in (page.get("corporate_actions") or {}).values():
                out.update(a[k] for a in actions for k in ACTION_SYMBOL_KEYS if a.get(k))
    return out


def candidate_sources(config_path: str, data_dir: Path) -> dict[str, list[str]]:
    """symbol -> where it came from, for every symbol discovery will try."""
    cfg = _config(config_path)
    sources: dict[str, list[str]] = {}
    for name, symbols in (("brute_force", brute_force_symbols(cfg["universe"]["max_symbol_letters"])),
                          ("assets", asset_symbols(data_dir)), ("corporate_actions", action_symbols(data_dir)),
                          ("config", cfg["alpaca"]["symbols"])):
        for s in symbols:
            sources.setdefault(s, []).append(name)
    return {s: v for s, v in sources.items() if VALID_SYMBOL.match(s)}


def probe(c: httpx.Client, symbols: list[str], start: str, end: str, pace: float) -> tuple[list[dict], list[str]]:
    """Monthly bars for `symbols` as [{symbols, pages}], plus symbols the vendor rejected. A rejected batch is
    split in half until the offending symbols are isolated."""
    params = {"symbols": ",".join(symbols), "timeframe": "1Month", "start": start, "end": end, "feed": "sip",
              "adjustment": "raw", "limit": 10000}
    try:
        return [{"symbols": symbols, "pages": fetch_pages(c, "/v2/stocks/bars", params, min_interval=pace)}], []
    except httpx.HTTPStatusError as e:
        if e.response.status_code not in (400, 422):
            raise
        if len(symbols) == 1:
            return [], symbols
        half = len(symbols) // 2
        (a, ra), (b, rb) = probe(c, symbols[:half], start, end, pace), probe(c, symbols[half:], start, end, pace)
        return a + b, ra + rb


def summarize(payloads: list[dict], sources: dict[str, list[str]]) -> pl.DataFrame:
    """One row per symbol with bars: months with a bar, first and last month, and where the symbol came from."""
    rows = [{"symbol": sym, "month": datetime.fromisoformat(b["t"]).date()}
            for p in payloads for req in p["requests"] for page in req["pages"]
            for sym, bars in (page.get("bars") or {}).items() for b in bars]
    if not rows:
        return pl.DataFrame(schema={"symbol": pl.String, "n_months": pl.UInt32, "first_month": pl.Date,
                                    "last_month": pl.Date, "sources": pl.List(pl.String)})
    found = (pl.DataFrame(rows).unique()
             .group_by("symbol").agg(n_months=pl.len().cast(pl.UInt32), first_month=pl.col("month").min(),
                                     last_month=pl.col("month").max()))
    tags = pl.DataFrame({"symbol": list(sources), "sources": list(sources.values())},
                        schema={"symbol": pl.String, "sources": pl.List(pl.String)})
    return found.join(tags, on="symbol", how="left").sort("symbol")


def returned_symbols(payload: dict) -> set[str]:
    """Symbols the vendor returned bars under, in a probe payload or a daily-bars raw payload."""
    pages = [pg for r in payload.get("requests", [payload]) for pg in r.get("pages", [])]
    return {s for pg in pages for s, bars in (pg.get("bars") or {}).items() if bars}


def discover(config_path: str, now: datetime | None = None, recheck_rounds: int = 3,
             stop_by: datetime | None = None) -> Path:
    """Probe every candidate symbol; write one raw file per batch and the derived candidate list.

    Alpaca returns a security once per request, under one of the requested symbols, when several of them map to
    it (SIVB + SIVBQ -> SIVBQ only; FB + META -> META only, which drops the ETF that reused FB). So after the
    main pass, the listed and corporate-action symbols that returned nothing are probed again in shuffled
    batches without their partners, up to recheck_rounds times or until a round finds nothing new.

    Stays out of the collection window (stop_by, default: 08:00 ET of the next session day): it refuses to start
    with less than DISCOVER_MIN_TIME left, and fails rather than run into the window (rerun after the close)."""
    stop_by = stop_by or default_stop_by()
    if stop_by - _now() < DISCOVER_MIN_TIME:
        raise RuntimeError(f"discover needs {DISCOVER_MIN_TIME} before the collection window "
                           f"({stop_by.astimezone(NY):%Y-%m-%d %H:%M} ET); run it after the close")
    cfg = _config(config_path)["universe"]
    data_dir, now = _data_dir(), now or datetime.now(timezone.utc)
    sources = candidate_sources(config_path, data_dir)
    end = (now - timedelta(minutes=20)).isoformat()  # the free plan withholds the latest 15 minutes
    pace, size, payloads = 60 / cfg["requests_per_minute"], cfg["probe_batch"], []
    at = datetime.min.replace(tzinfo=timezone.utc)

    def run_pass(name: str, symbols: list[str]) -> set[str]:
        nonlocal at
        print(f"discover {name}: {len(symbols)} symbols in {-(-len(symbols) // size)} batches")
        found = set()
        for i in range(0, len(symbols), size):
            if _now() >= stop_by - STOP_MARGIN:
                raise RuntimeError(f"discover stopped before the collection window "
                                   f"({stop_by.astimezone(NY):%H:%M} ET); rerun after the close")
            # Raw files are named by ingested_at; the Windows clock can repeat a value between fast batches,
            # which would overwrite the previous file.
            t0, at = time.monotonic(), max(datetime.now(timezone.utc), at + timedelta(microseconds=1))
            requests, rejected = probe(c, symbols[i:i + size], HISTORY_START, end, pace)
            payload = {"ingested_at": at.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
                       "pass": name, "timeframe": "1Month", "start": HISTORY_START, "end": end,
                       "requests": requests, "rejected": rejected}
            write_raw(data_dir, "alpaca", "universe_probe", payload, at)
            payloads.append(payload)
            found |= returned_symbols(payload)
            if i // size % 50 == 0:
                print(f"  batch {i // size}: {symbols[i]}..")
        return found

    with client() as c:
        found = run_pass("main", sorted(sources))
        missing = sorted(s for s, v in sources.items() if v != ["brute_force"] and s not in found)
        for n in range(1, recheck_rounds + 1):
            if not missing:
                break
            random.Random(n).shuffle(missing)
            new = run_pass(f"recheck_{n}", missing)
            print(f"discover recheck_{n}: {len(new)} more symbols with bars")
            if not new:
                break
            found |= new
            missing = sorted(set(missing) - new)
    found = summarize(payloads, sources)
    path = write_parquet(found, data_dir / CANDIDATES)
    print(f"discover: {found.height} symbols with bars of {len(sources)} tried; "
          f"{sum(len(p['rejected']) for p in payloads)} rejected -> {path}")
    return path


def _month(col: str | date) -> pl.Expr:
    c = pl.col(col) if isinstance(col, str) else pl.lit(col)
    return c.dt.year().cast(pl.Int32) * 12 + c.dt.month().cast(pl.Int32)


def plan_batches(candidates: pl.DataFrame, start: date, end: date, max_bars: int, max_symbols: int,
                 seed: int | None = None) -> list[list[str]]:
    """Symbols with bars inside [start, end] grouped so each batch holds about max_bars daily bars and at most
    max_symbols symbols (URL length). Bars are estimated from the months the probe saw, overlapping [start, end].
    In symbol order, or shuffled with `seed` (rechecks: keep symbols of one security apart)."""
    first, last = date(start.year, start.month, 1), date(end.year, end.month, 1)
    overlap = pl.min_horizontal(_month("last_month"), _month(last)) - pl.max_horizontal(_month("first_month"),
                                                                                         _month(first)) + 1
    est = (candidates.filter((pl.col("last_month") >= first) & (pl.col("first_month") <= last))
           .with_columns(bars=pl.min_horizontal(pl.col("n_months").cast(pl.Int32), overlap) * SESSIONS_PER_MONTH)
           .sort("symbol"))
    rows = est.select("symbol", "bars").rows()
    if seed is not None:
        random.Random(seed).shuffle(rows)
    batches, batch, total = [], [], 0
    for sym, bars in rows:
        if batch and (total + bars > max_bars or len(batch) >= max_symbols):
            batches.append(batch)
            batch, total = [], 0
        batch.append(sym)
        total += bars
    return batches + ([batch] if batch else [])


def coverage(done_dir: Path) -> dict[str, date]:
    """symbol -> the last day an earlier backfill fetched it through (the batch returned bars for it)."""
    out: dict[str, date] = {}
    for f in done_dir.glob("*"):
        m = json.loads(f.read_text(encoding="utf-8"))
        if "end" not in m:
            continue
        end = date.fromisoformat(m["end"])
        for s in m.get("returned", []):
            out[s] = max(out.get(s, end), end)
    return out


def backfill(config_path: str, start: date, end: date, run_id: str | None = None, recheck_rounds: int = 3,
             stop_by: datetime | None = None) -> dict:
    """Daily bars for every discovered candidate over [start, end], one raw file per batch.

    Incremental: a symbol an earlier backfill already fetched is fetched only from the day after that run's end
    (a quarterly run tops up what eod may have missed, plus whole histories for newly discovered symbols).
    Resumable: a batch that succeeded leaves a marker in cache/ (its range and the symbols it returned) and is
    skipped on a rerun; a failed batch is reported and retried on the next run. Symbols that returned nothing
    because another symbol of the same security was in their batch (see discover) are fetched again in shuffled
    batches, up to recheck_rounds times or until a round returns nothing new.

    Stays out of the collection window: no batch starts after stop_by (default: 08:00 ET of the next session day)
    minus STOP_MARGIN. A stopped run says so (stats["stopped"]) and the next run carries on from the markers."""
    stop_by = stop_by or default_stop_by()
    cfg = _config(config_path)["universe"]
    data_dir = _data_dir()
    run_id = run_id or f"universe_backfill__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    done_dir = data_dir / BACKFILL_DONE
    done_dir.mkdir(parents=True, exist_ok=True)
    covered = coverage(done_dir)
    candidates = (pl.read_parquet(data_dir / CANDIDATES)
                  .with_columns(from_day=pl.col("symbol").replace_strict(
                      {s: d + timedelta(days=1) for s, d in covered.items()}, default=start, return_dtype=pl.Date))
                  .with_columns(from_day=pl.max_horizontal("from_day", pl.lit(start)))
                  .filter(pl.col("from_day") <= end))
    stats = {"batches": 0, "failed": 0, "rows": 0, "stopped": False}

    def plan(subset: pl.DataFrame, seed: int | None = None) -> list[tuple[date, list[str]]]:
        return [(day, b) for (day,), group in subset.group_by("from_day", maintain_order=True)
                for b in plan_batches(group, day, end, cfg["backfill_bars_per_batch"], cfg["probe_batch"], seed)]

    def run_batches(name: str, batches: list[tuple[date, list[str]]]) -> set[str]:
        got = set()
        print(f"backfill {name}: {len(batches)} batches, {sum(len(b) for _, b in batches)} symbols, ..{end}")
        for i, (day, batch) in enumerate(batches):
            marker = done_dir / hashlib.sha1(f"{day}|{end}|{','.join(batch)}".encode()).hexdigest()[:16]
            if marker.exists():
                got |= set(json.loads(marker.read_text()).get("returned", batch))
                continue
            if _now() >= stop_by - STOP_MARGIN:
                stats["stopped"] = True
                print(f"  stopped before the collection window ({stop_by.astimezone(NY):%Y-%m-%d %H:%M} ET), "
                      f"{len(batches) - i} batches left for the next run")
                break
            sessions = [s.date() for s in calendar().sessions_in_range(day, end)]
            try:
                rec = pipeline.collect_daily_bars(config_path, sessions, run_id=f"{run_id}__{name}_b{i:04d}",
                                                  symbols=batch)
            except Exception as e:  # recorded in pipeline_runs by collect_daily_bars; the next run retries it
                stats["failed"] += 1
                print(f"  batch {i} ({batch[0]}..{batch[-1]}) failed: {e!r}"[:300])
                continue
            keys = returned_symbols(json.loads(Path(rec["raw_path"]).read_text(encoding="utf-8")))
            got |= keys
            stats["batches"] += 1
            stats["rows"] += rec["rows"]
            marker.write_text(json.dumps({"run_id": rec["run_id"], "start": str(day), "end": str(end),
                                          "rows": rec["rows"], "symbols": len(batch), "returned": sorted(keys)}))
            print(f"  batch {i}/{len(batches)} {day} {batch[0]}..{batch[-1]}: {rec['rows']} rows, "
                  f"{len(batch) - len(keys)} symbols empty, {rec['quarantined']} quarantined, {rec['cycle_s']} s")
        return got

    planned = plan(candidates)
    wanted = {s for _, b in planned for s in b}
    returned = run_batches("main", planned)
    for n in range(1, recheck_rounds + 1):
        missing = candidates.filter(pl.col("symbol").is_in(sorted(wanted - returned)))
        if missing.is_empty() or stats["failed"] or stats["stopped"]:  # rechecks once every main batch is in
            break
        new = run_batches(f"recheck_{n}", plan(missing, seed=n)) - returned
        print(f"backfill recheck_{n}: {len(new)} more symbols with bars")
        if not new:
            break
        returned |= new
    stats |= {"wanted": len(wanted), "returned": len(returned), "new_symbols": len(wanted - set(covered))}
    print(f"backfill: {'stopped' if stats['stopped'] else 'done'}, {len(returned)} of {len(wanted)} symbols "
          f"returned bars, {stats['failed']} failed batches" + (" (rerun to continue)" if stats["failed"]
                                                               or stats["stopped"] else ""))
    return stats


def last_closed_session(now: datetime | None = None) -> date:
    """The latest session whose close is past the free plan's 15-minute delay."""
    now = now or datetime.now(timezone.utc)
    cal = calendar()
    s = cal.date_to_session(now.astimezone(NY).date().isoformat(), "previous")
    if cal.session_close(s).to_pydatetime() > now - timedelta(minutes=20):
        s = cal.previous_session(s)
    return s.date()


def refresh(config_path: str, now: datetime | None = None) -> dict:
    """Rediscover the whole market, then backfill what is new (the universe_refresh DAG, quarterly)."""
    stop_by = default_stop_by()
    candidates = pl.read_parquet(discover(config_path, now, stop_by=stop_by))
    stats = backfill(config_path, date.fromisoformat(HISTORY_START), last_closed_session(now), stop_by=stop_by)
    return {"candidates": candidates.height, **stats}


def recent_symbols(config_path: str) -> list[str]:
    """What eod refetches daily bars for: active non-OTC assets plus the configured symbols."""
    return sorted(asset_symbols(_data_dir(), active_only=True) | set(_config(config_path)["alpaca"]["symbols"]))


def collect_recent_daily_bars(config_path: str, sessions: list[date], run_id: str) -> list[dict]:
    """eod: the last sessions' daily bars for the whole market, one raw file per batch."""
    symbols, size = recent_symbols(config_path), _config(config_path)["universe"]["recent_batch"]
    return [pipeline.collect_daily_bars(config_path, sessions, run_id=f"{run_id}__b{i // size:03d}",
                                        symbols=symbols[i:i + size])
            for i in range(0, len(symbols), size)]


if __name__ == "__main__":
    if sys.argv[1] == "refresh":
        print(refresh(sys.argv[2]))
    elif sys.argv[1] == "discover":
        discover(sys.argv[2])
    elif sys.argv[1] == "backfill":
        backfill(sys.argv[2], date.fromisoformat(sys.argv[3]), date.fromisoformat(sys.argv[4]))
