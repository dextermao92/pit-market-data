"""Security identity for the whole-market daily bars (Stage 2 P1b, docs/stage2/p1b-curate-design.md).

The vendor serves bars by symbol, not by security: a renamed security's whole history comes back under its new
symbol and its as-traded part under the old one (identical bars, "twins"), and an old symbol can later belong to
a different security. This turns symbols into securities:

1. traded bars only (volume > 0); zero-volume filler bars are not prices;
2. each symbol's traded series is cut into segments at long gaps (> 250 sessions), at 61-250 session gaps with
   a 2x price move, and at the edges of a twin run when the symbol that stops first (the old name) also trades
   on its own while its twin trades too, before or after the run: two different bars on one day cannot be one
   security, so the old name carried another security then (a ticker handed over in a merger, or reused);
3. segments sharing >= 5 identical bars are linked (unless both carry on smoothly after the shared run), and so
   are a name change's old and new symbols when the new one starts where the old one stops;
4. a security is a connected group of segments; on each session its as-traded symbol is the segment whose
   series ends first (the old name stops at the rename, the successor carries on), ties broken by rule.

Identity uses hindsight on purpose: that FB became META is a fact about the past, not a signal. The daily universe
built on top of it must only use sessions before each date.

Memory: the whole market is ~29M bars. The rules only need (symbol id, session, close, bar fingerprint), so
`resolve` works on that narrow integer frame (~0.7 GB) and full OHLCV is attached afterwards. `run` loads and
writes in symbol buckets so no step holds every bar with every column (the first version needed 14 GB and was
killed inside the Airflow container). `build` is the same pipeline on an in-memory frame, for tests.
"""

import hashlib
import json
import math
import os
import sys
from datetime import date
from pathlib import Path

import polars as pl

from pit.storage import write_parquet

MIN_SHARED = 5               # identical bars that make two segments twins
LONG_GAP = 250               # sessions without a trade that always start a new segment
MID_GAP = 60                 # ... and from here, only with a price move of JUMP or more
JUMP = math.log(2)
SMOOTH = math.log(1.5)       # a next close within x1.5 of the last shared close carries on "smoothly"
STOP_WITHIN = 60             # no trade within this many sessions after a twin run: the symbol stopped
LINK_WINDOW = 3              # sessions around a name change for the old end / new start
END_WINDOW = 10              # days between the last trade and an ending action
ACTIVE_WITHIN = 5            # sessions before the last one: still trading
BUCKETS = 8                  # symbol buckets for loading and writing (memory)
END_TYPES = {"cash_merger": "cash_merger", "stock_merger": "stock_merger", "stock_and_cash_merger": "stock_merger",
             "worthless_removal": "worthless", "name_change": "renamed"}
RENAME_TYPES = ("name_change", "forward_split", "reverse_split")
PRICES = ("open", "high", "low", "close", "vwap")
DAY_COLUMNS = ("security_id", "session", "symbol", "fetch_symbol", *PRICES, "volume", "trade_count", "no_trade")


def _calendar(sessions: list[date]) -> pl.DataFrame:
    return pl.DataFrame({"day": sessions}, schema={"day": pl.Date}).with_columns(sid=pl.int_range(pl.len(), dtype=pl.Int32))


def _slim(bars: pl.DataFrame | pl.LazyFrame, cal: pl.DataFrame) -> pl.LazyFrame:
    """Traded bars as (symbol, sid, close, key): key fingerprints the whole bar (session and OHLCV)."""
    return (bars.lazy().filter(pl.col("volume") > 0).join(cal.lazy(), on="day")
            .select("symbol", "sid", "close", key=pl.struct("day", "open", "high", "low", "close", "volume").hash()))


def _with_ids(slim: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Replaces the symbol string by an integer id; returns (frame, lookup sym -> symbol)."""
    lookup = slim.select(pl.col("symbol").unique().sort()).with_columns(sym=pl.int_range(pl.len(), dtype=pl.UInt32))
    return (slim.join(lookup, on="symbol").select("sym", "sid", "close", "key").sort("sym", "sid"), lookup)


def _twin_runs(tr: pl.DataFrame, unit: str) -> pl.DataFrame:
    """(a, b, shared, f, l, a_state, b_state) for every pair of `unit` (sym or seg) sharing >= MIN_SHARED identical
    bars; state = what the unit does after the last shared session: stops / smooth / jumps."""
    multi = tr.select(unit, "sid", "key").filter(pl.col("key").is_duplicated())  # bars held by 2+ symbols
    runs = (multi.join(multi, on=["key", "sid"], suffix="_b").filter(pl.col(unit) < pl.col(f"{unit}_b"))
            .group_by(a=unit, b=f"{unit}_b").agg(shared=pl.len(), f=pl.col("sid").min(), l=pl.col("sid").max())
            .filter(pl.col("shared") >= MIN_SHARED))
    in_runs = pl.concat([runs["a"], runs["b"]]).unique().implode()
    series = tr.filter(pl.col(unit).is_in(in_runs)).select(u=pl.col(unit), sid="sid", close="close")
    nxt = series.rename({"sid": "ns", "close": "nc"}).sort("ns")
    for side in ("a", "b"):
        q = (runs.select("a", "b", "l", u=side).join(series, left_on=["u", "l"], right_on=["u", "sid"], how="left")
             .sort("l").join_asof(nxt, left_on="l", right_on="ns", by="u", strategy="forward",
                                  allow_exact_matches=False, check_sortedness=False))
        q = q.select("a", "b", pl.when(pl.col("ns").is_null() | (pl.col("ns") - pl.col("l") > STOP_WITHIN))
                     .then(pl.lit("stops"))
                     .when((pl.col("nc") / pl.col("close")).log().abs() < SMOOTH).then(pl.lit("smooth"))
                     .otherwise(pl.lit("jumps")).alias(f"{side}_state"))
        runs = runs.join(q, on=["a", "b"])
    return runs


def _twin_cuts(tr: pl.DataFrame, runs: pl.DataFrame) -> pl.DataFrame:
    """(sym, sid, twin_cut): where the old name of a twin pair (the one that stops or jumps at the end of the
    shared run while the other carries on) must be cut because both symbols trade, with different bars, on the
    same sessions before the run (cut at its first shared bar) or after it (cut at its first bar after)."""
    old_a = (pl.col("a_state").is_in(["stops", "jumps"]) & (pl.col("b_state") == "smooth")) | (
        (pl.col("a_state") == "stops") & (pl.col("b_state") == "jumps"))
    old_b = (pl.col("b_state").is_in(["stops", "jumps"]) & (pl.col("a_state") == "smooth")) | (
        (pl.col("b_state") == "stops") & (pl.col("a_state") == "jumps"))
    pairs = pl.concat([runs.filter(old_a).select("f", "l", x="a", y="b"), runs.filter(old_b).select("f", "l", x="b", y="a")])
    s = tr.filter(pl.col("sym").is_in(pl.concat([pairs["x"], pairs["y"]]).unique().implode())).select("sym", "sid")
    cuts = []
    for name, outside in (("twin_start", pl.col("sid") < pl.col("f")), ("twin_end", pl.col("sid") > pl.col("l"))):
        xs = pairs.join(s, left_on="x", right_on="sym").filter(outside)
        ys = pairs.join(s, left_on="y", right_on="sym").filter(outside)
        hit = xs.join(ys, on=["x", "y", "f", "l", "sid"], how="semi").select("x", "f", "l").unique()
        if name == "twin_start":
            cuts.append(hit.select(sym="x", sid="f", twin_cut=pl.lit(name)))
        else:
            cuts.append(hit.join(s, left_on="x", right_on="sym").filter(pl.col("sid") > pl.col("l"))
                        .group_by("x", "l").agg(pl.col("sid").min()).select(sym="x", sid="sid", twin_cut=pl.lit(name)))
    return pl.concat(cuts).unique(["sym", "sid"])


def _segments(tr: pl.DataFrame, symbol_runs: pl.DataFrame) -> pl.DataFrame:
    """Adds seg (sym * 10,000 + segment number) and cut (the rule that started it, null for a first segment)."""
    tr = (tr.with_columns(gap=pl.col("sid").diff().over("sym"),
                          move=(pl.col("close") / pl.col("close").shift().over("sym")).log().abs())
          .join(_twin_cuts(tr, symbol_runs), on=["sym", "sid"], how="left").sort("sym", "sid"))
    cut = (pl.when(pl.col("gap") > LONG_GAP).then(pl.lit("long_gap"))
           .when((pl.col("gap") > MID_GAP) & (pl.col("move") > JUMP)).then(pl.lit("gap_jump"))
           .otherwise(pl.col("twin_cut")))
    n = pl.col("cut").is_not_null().cum_sum().over("sym").cast(pl.Int64)
    return (tr.with_columns(cut=cut).with_columns(seg=pl.col("sym").cast(pl.Int64) * 10_000 + n)
            .drop("gap", "move", "twin_cut"))


def _event_links(segs: pl.DataFrame, events: pl.DataFrame, sessions: pl.DataFrame) -> pl.DataFrame:
    """(a, b) segment pairs joined by a name change (or a split that changes the symbol): the old symbol's segment
    ends within LINK_WINDOW sessions before the change and the new symbol's starts around it, at a similar price
    (or across a split)."""
    ev = (events.filter(pl.col("type").is_in(RENAME_TYPES) & pl.col("new_symbol").is_not_null()
                        & (pl.col("new_symbol") != pl.col("symbol")))
          .sort("date").join_asof(sessions.sort("day"), left_on="date", right_on="day", strategy="forward")
          .drop_nulls("sid").select("type", old="symbol", new="new_symbol", d="sid"))
    split_days = (events.filter(pl.col("type").is_in(["forward_split", "reverse_split"])).sort("date")
                  .join_asof(sessions.sort("day"), left_on="date", right_on="day", strategy="forward")
                  .select("symbol", "sid").drop_nulls())
    old = segs.select(old="symbol", a="seg", a_last="last", a_close="last_close")
    new = segs.select(new="symbol", b="seg", b_first="first", b_close="first_close")
    links = (ev.join(old, on="old").filter((pl.col("a_last") >= pl.col("d") - LINK_WINDOW) & (pl.col("a_last") <= pl.col("d")))
             .join(new, on="new").filter((pl.col("b_first") >= pl.col("d") - 1) & (pl.col("b_first") <= pl.col("d") + LINK_WINDOW)))
    near_split = (links.select("a", "b", "old", "new", "d").join(split_days, left_on="old", right_on="symbol", how="left")
                  .filter((pl.col("sid") - pl.col("d")).abs() <= LINK_WINDOW).select("a", "b").unique())
    smooth = links.filter(((pl.col("b_close") / pl.col("a_close")).log().abs() < SMOOTH)
                          | pl.col("type").is_in(["forward_split", "reverse_split"])).select("a", "b")
    return pl.concat([smooth, near_split]).unique()


def _components(nodes: list[int], edges: list[tuple[int, int]]) -> dict[int, int]:
    parent = {n: n for n in nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    return {n: find(n) for n in nodes}


def _security_id(symbol: str, first: date) -> str:
    return hashlib.sha1(f"{symbol}|{first}".encode()).hexdigest()[:16]


def resolve(slim: pl.DataFrame, lookup: pl.DataFrame, events: pl.DataFrame, cal: pl.DataFrame) -> dict:
    """The identity rules on the narrow frame (sym, sid, close, key), sorted by (sym, sid). Returns `mapping` (one
    row per traded security-day: sym, sid, comp, fetch_sym), `gaps` (no-trade security-days), `securities`,
    `security_symbols`, `ids` (comp -> security_id) and the report."""
    last_sid = cal.height - 1
    sym_runs = _twin_runs(slim, "sym")
    tr = _segments(slim, sym_runs)
    del slim
    segs = (tr.group_by("seg").agg(sym=pl.col("sym").first(), first=pl.col("sid").min(), last=pl.col("sid").max(),
                                   first_close=pl.col("close").sort_by("sid").first(),
                                   last_close=pl.col("close").sort_by("sid").last(), n=pl.len())
            .join(lookup, on="sym"))

    runs = _twin_runs(tr, "seg")
    smooth_twins = runs.filter((pl.col("a_state") == "smooth") & (pl.col("b_state") == "smooth"))
    twin_links = runs.join(smooth_twins, on=["a", "b"], how="anti").select("a", "b")
    ev_links = _event_links(segs, events, cal)
    comp = _components(segs["seg"].to_list(), list(twin_links.iter_rows()) + list(ev_links.iter_rows()))
    segs = segs.with_columns(comp=pl.col("seg").replace_strict(comp))
    comp_of = segs.select("seg", "comp")
    cuts = tr.group_by("cut").len().drop_nulls().sort("cut").to_dicts()
    cut_rows = tr.select("sym", "seg", "cut").with_columns(prev_seg=pl.col("seg").shift().over("sym")).filter(
        pl.col("cut").str.starts_with("twin_"))
    n_traded_in = tr.height

    # As-traded symbol per (security, session), only where a security has more than one segment: the candidate
    # segment that ends first, then the old symbol of a later rename, then no bankruptcy Q suffix, then the shorter
    # symbol, then alphabetical. Most securities are one segment and keep every bar as is.
    multi = segs.filter(pl.len().over("comp") > 1).select("seg", "comp", "symbol", seg_last="last")
    renamed_later = (events.filter(pl.col("type").is_in(RENAME_TYPES)).sort("date")
                     .join_asof(cal.sort("day"), left_on="date", right_on="day", strategy="forward")
                     .select(symbol="symbol", rename_sid="sid").drop_nulls().group_by("symbol").agg(pl.col("rename_sid").max()))
    cand = (tr.select("seg", "sym", "sid", "key").join(multi, on="seg")
            .join(renamed_later, on="symbol", how="left")
            .with_columns(old_name=(pl.col("rename_sid") >= pl.col("sid")).fill_null(False),
                          q=pl.col("symbol").str.contains(r"^[A-Z]{4}Q$"), sym_len=pl.col("symbol").str.len_chars())
            .sort(["comp", "sid", "seg_last", "old_name", "q", "sym_len", "symbol"],
                  descending=[False, False, False, True, False, False, False]))
    picked = cand.group_by("comp", "sid", maintain_order=True).agg(
        seg=pl.col("seg").first(), n_keys=pl.col("key").n_unique(),
        tie=(pl.col("seg_last") == pl.col("seg_last").first()).sum() > 1,
        fetch_sym=pl.col("sym").sort_by("seg_last", descending=True).first())
    del cand
    single = (tr.select("seg", "sym", "sid", "key").join(comp_of.join(multi.select("seg"), on="seg", how="anti"), on="seg")
              .with_columns(fetch_sym=pl.col("sym"), tie=pl.lit(False), conflict=pl.lit(False)))
    chosen = tr.select("seg", "sym", "sid", "key").join(
        picked.select("seg", "sid", "comp", "fetch_sym", "tie", conflict=pl.col("n_keys") > 1), on=["seg", "sid"])
    del tr, picked
    per_day = pl.concat([single, chosen], how="diagonal_relaxed").drop("seg")
    del single, chosen

    dup = (per_day.select("key", "comp").filter(pl.col("key").is_duplicated())
           .group_by("key").agg(n=pl.col("comp").n_unique()).filter(pl.col("n") > 1).height)
    tie_days, conflict_days = int(per_day["tie"].sum()), int(per_day["conflict"].sum())
    stats = per_day.group_by("comp").agg(first_sid=pl.col("sid").min(), last_sid=pl.col("sid").max(),
                                         first_sym=pl.col("sym").sort_by("sid").first(),
                                         last_sym=pl.col("sym").sort_by("sid").last(),
                                         tie_days=pl.col("tie").sum(), conflict_days=pl.col("conflict").sum())
    mapping = per_day.select("sym", "sid", "comp", "fetch_sym").sort("comp", "sid")
    del per_day

    firsts = (stats.join(lookup.rename({"sym": "first_sym", "symbol": "first_symbol"}), on="first_sym")
              .join(lookup.rename({"sym": "last_sym", "symbol": "last_symbol"}), on="last_sym")
              .join(cal.rename({"sid": "first_sid", "day": "first_session"}), on="first_sid")
              .join(cal.rename({"sid": "last_sid", "day": "last_session"}), on="last_sid"))
    firsts = firsts.with_columns(security_id=pl.struct("first_symbol", "first_session").map_elements(
        lambda r: _security_id(r["first_symbol"], r["first_session"]), return_dtype=pl.String))
    ids = firsts.select("comp", "security_id")

    gaps = (mapping.with_columns(nxt=pl.col("sid").shift(-1).over("comp"))
            .filter(pl.col("nxt") - pl.col("sid") > 1)
            .select("comp", "sym", "fetch_sym", sid=pl.int_ranges(pl.col("sid") + 1, "nxt", dtype=pl.Int32))
            .explode("sid", empty_as_null=True))

    change = ((pl.col("sym") != pl.col("sym").shift()) | (pl.col("fetch_sym") != pl.col("fetch_sym").shift())
              | (pl.col("comp") != pl.col("comp").shift())).fill_null(True)
    symbols = (mapping.with_columns(run=change.cum_sum())
               .group_by("run").agg(pl.col("comp", "sym", "fetch_sym").first(), first=pl.col("sid").min(), last=pl.col("sid").max())
               .join(ids, on="comp").join(lookup, on="sym")
               .join(lookup.rename({"sym": "fetch_sym", "symbol": "fetch_symbol"}), on="fetch_sym")
               .join(cal.rename({"sid": "first", "day": "valid_from"}), on="first")
               .join(cal.rename({"sid": "last", "day": "valid_to"}), on="last")
               .select("security_id", "symbol", "fetch_symbol", "valid_from", "valid_to").sort("security_id", "valid_from"))

    securities = _ends(firsts, events, symbols, last_sid)
    cut_flags = (pl.concat([cut_rows.select("seg"), cut_rows.select(seg="prev_seg")])  # both sides of the cut
                 .join(comp_of, on="seg").join(ids, on="comp").select("security_id").unique())
    smooth_ids = (pl.concat([smooth_twins.select(seg="a"), smooth_twins.select(seg="b")]).join(comp_of, on="seg")
                  .join(ids, on="comp").select("security_id").unique())
    securities = securities.with_columns(flags=pl.concat_list(
        pl.when(pl.col("tie_days") > 0).then(pl.lit("tie")),
        pl.when(pl.col("conflict_days") > 0).then(pl.lit("conflict")),
        pl.when(pl.col("security_id").is_in(cut_flags["security_id"].implode())).then(pl.lit("twin_cut")),
        pl.when(pl.col("security_id").is_in(smooth_ids["security_id"].implode())).then(pl.lit("smooth_twin")),
    ).list.drop_nulls())

    report = {
        "symbols": lookup.height, "traded_bars": n_traded_in, "segments": segs.height, "cuts": cuts,
        "twin_pairs_symbols": sym_runs.height, "twin_pairs_segments": runs.height,
        "smooth_twins_unlinked": smooth_twins.height, "event_links": ev_links.height,
        "securities": securities.height, "security_days": mapping.height + gaps.height,
        "traded_security_days": mapping.height, "bars_merged_away": n_traded_in - mapping.height,
        "tie_days": tie_days, "conflict_days": conflict_days, "bars_in_two_securities": dup,
        "end_reasons": securities.group_by("end_reason").len().sort("end_reason").to_dicts(),
        "flags": securities.explode("flags", empty_as_null=True).group_by("flags").len().drop_nulls().sort("flags").to_dicts(),
    }
    return {"mapping": mapping, "gaps": gaps, "ids": ids, "securities": securities, "security_symbols": symbols,
            "report": report}


def _ends(firsts: pl.DataFrame, events: pl.DataFrame, symbols: pl.DataFrame, last_sid: int) -> pl.DataFrame:
    """How each security ended: an ending action on its last symbol within END_WINDOW days of its last trade."""
    rank = {t: i for i, t in enumerate(END_TYPES)}
    match = (firsts.select("comp", "last_symbol", "last_session")
             .join(events.filter(pl.col("type").is_in(list(END_TYPES))).select("symbol", "type", "new_symbol", "date"),
                   left_on="last_symbol", right_on="symbol")
             .with_columns(dist=(pl.col("date") - pl.col("last_session")).dt.total_days().abs())
             .filter(pl.col("dist") <= END_WINDOW)
             .with_columns(r=pl.col("type").replace_strict(rank))
             # deterministic: type rank, nearest date, an action naming a successor, then the symbol
             .sort(["r", "dist", "new_symbol"], nulls_last=True, maintain_order=True)
             .group_by("comp", maintain_order=True).agg(pl.col("type", "new_symbol", "date").first()))
    ends = firsts.join(match, on="comp", how="left")
    succ = (ends.filter(pl.col("new_symbol").is_not_null()).select("security_id", "new_symbol", "date")
            .join(symbols.select(succ_id="security_id", new_symbol="symbol", valid_from="valid_from", valid_to="valid_to"), on="new_symbol")
            .filter((pl.col("valid_to") >= pl.col("date")) & (pl.col("succ_id") != pl.col("security_id")))
            .sort("valid_from", "succ_id").group_by("security_id", maintain_order=True)
            .agg(successor_id=pl.col("succ_id").first()))
    reason = (pl.when(pl.col("last_sid") >= last_sid - ACTIVE_WITHIN).then(pl.lit("active"))
              .when(pl.col("type").is_not_null()).then(pl.col("type").replace_strict(END_TYPES, default=None))
              .otherwise(pl.lit("delisted_unknown")))
    return (ends.with_columns(end_reason=reason).join(succ, on="security_id", how="left")
            .select("security_id", "first_symbol", "last_symbol", "first_session", "last_session", "end_reason",
                    "successor_id", "tie_days", "conflict_days"))


def _days(ident: dict, bars: pl.DataFrame, lookup: pl.DataFrame, cal: pl.DataFrame, syms=None) -> pl.DataFrame:
    """security_days rows for the traded days whose as-traded symbol is in `bars` (optionally only `syms`), with
    full OHLCV from `bars` (symbol, day, ...)."""
    m = ident["mapping"] if syms is None else ident["mapping"].filter(pl.col("sym").is_in(syms))
    b = bars.join(lookup, on="symbol").join(cal, on="day").select("sym", "sid", *PRICES, "volume", "trade_count")
    return (m.join(b, on=["sym", "sid"]).with_columns(no_trade=pl.lit(False))
            .pipe(_named, ident, lookup, cal))


def _gap_days(ident: dict, lookup: pl.DataFrame, cal: pl.DataFrame) -> pl.DataFrame:
    g = ident["gaps"].with_columns(*(pl.lit(None, pl.Float64).alias(c) for c in PRICES), volume=pl.lit(0, pl.Int64),
                                   trade_count=pl.lit(0, pl.Int64), no_trade=pl.lit(True))
    return _named(g, ident, lookup, cal)


def _named(df: pl.DataFrame, ident: dict, lookup: pl.DataFrame, cal: pl.DataFrame) -> pl.DataFrame:
    return (df.join(ident["ids"], on="comp").join(lookup, on="sym")
            .join(lookup.rename({"sym": "fetch_sym", "symbol": "fetch_symbol"}), on="fetch_sym")
            .join(cal, on="sid").rename({"day": "session"})
            .select(*DAY_COLUMNS))


def build(bars: pl.DataFrame, events: pl.DataFrame, sessions: list[date]) -> dict:
    """In-memory pipeline (tests, small data). bars: one row per (symbol, day) with OHLCV, vwap, trade_count.
    events: staged corporate_events, latest version per id. sessions: every session covering the bars."""
    cal = _calendar(sessions)
    slim, lookup = _with_ids(_slim(bars, cal).collect())
    ident = resolve(slim, lookup, events, cal)
    days = pl.concat([_days(ident, bars, lookup, cal), _gap_days(ident, lookup, cal)]).sort("security_id", "session")
    return {"security_days": days, "security_symbols": ident["security_symbols"], "securities": ident["securities"],
            "report": ident["report"]}


def _bars_files(data_dir: Path) -> list[Path]:
    # Files sort by fetch time and every row of a file shares it, so within (symbol, day) the last row in this
    # order is the latest version.
    return sorted((data_dir / "staged" / "daily_bars").glob("ingest_date=*/*.parquet"), key=lambda f: f.stem)


def _bucket(files: list[Path], b: int, columns: tuple[str, ...]) -> pl.LazyFrame:
    return (pl.scan_parquet(files).select("symbol", "bar_start", *columns)
            .filter(pl.col("symbol").hash(seed=0) % BUCKETS == b)
            .with_columns(day=pl.col("bar_start").dt.convert_time_zone("America/New_York").dt.date())
            .unique(["symbol", "day"], keep="last", maintain_order=True))


def load_events(data_dir: Path) -> pl.DataFrame:
    files = sorted((data_dir / "staged" / "corporate_events").glob("ingest_date=*/*.parquet"))
    return (pl.concat([pl.scan_parquet(f) for f in files], how="diagonal_relaxed")
            .sort("ingested_at").group_by("id").last().collect())


def run(data_dir: Path, out_dir: Path | None = None) -> dict:
    """Reads staged daily bars and corporate events under data_dir; writes data_dir/curated/identity (or out_dir)."""
    from pit.market_hours import calendar
    files = _bars_files(data_dir)
    span = pl.scan_parquet(files).select(pl.col("bar_start").min().alias("a"), pl.col("bar_start").max().alias("b")).collect()
    cal = _calendar([s.date() for s in calendar().sessions_in_range(
        span["a"].dt.convert_time_zone("America/New_York").dt.date().item(),
        span["b"].dt.convert_time_zone("America/New_York").dt.date().item())])
    slim = pl.concat([_slim(_bucket(files, b, ("open", "high", "low", "close", "volume")), cal).collect()
                      for b in range(BUCKETS)])
    held = list(_with_ids(slim))
    del slim
    lookup = held[1]
    ident = resolve(held.pop(0), lookup, load_events(data_dir), cal)  # resolve holds the only reference: it frees it

    root = out_dir or data_dir / "curated" / "identity"
    days_dir = root / "security_days"
    for old in days_dir.glob("*.parquet"):
        old.unlink()
    for b in range(BUCKETS):
        bars = _bucket(files, b, (*PRICES, "volume", "trade_count")).collect()
        write_parquet(_days(ident, bars, lookup, cal).sort("security_id", "session"), days_dir / f"part-{b}.parquet")
        del bars
    write_parquet(_gap_days(ident, lookup, cal), days_dir / "no_trade.parquet")
    for name in ("security_symbols", "securities"):
        write_parquet(ident[name], root / f"{name}.parquet")
    (root / "report.json").write_text(json.dumps(ident["report"], indent=2, default=str), encoding="utf-8")
    return ident["report"]


if __name__ == "__main__":
    print(json.dumps(run(Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "data"))), indent=2,
                     default=str))
