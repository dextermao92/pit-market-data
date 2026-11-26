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
built on top of it must only use sessions before each date. `build` is a pure function of its inputs; `run`
reads staged data and writes data/curated/identity/.
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
END_TYPES = {"cash_merger": "cash_merger", "stock_merger": "stock_merger", "stock_and_cash_merger": "stock_merger",
             "worthless_removal": "worthless", "name_change": "renamed"}
RENAME_TYPES = ("name_change", "forward_split", "reverse_split")
PRICES = ("open", "high", "low", "close", "vwap")


def _traded(bars: pl.DataFrame, sessions: pl.DataFrame) -> pl.DataFrame:
    return (bars.filter(pl.col("volume") > 0).join(sessions, on="day")
            .with_columns(key=pl.struct("day", "open", "high", "low", "close", "volume").hash())
            .sort("symbol", "sid"))


def _twin_runs(tr: pl.DataFrame, unit: str) -> pl.DataFrame:
    """(a, b, shared, f, l, a_state, b_state) for every pair of `unit` (symbol or seg) sharing >= MIN_SHARED
    identical bars; state = what the unit does after the last shared session: stops / smooth / jumps."""
    multi = (tr.group_by("key").agg(pl.col(unit), pl.col("sid").first())
             .filter(pl.col(unit).list.len() > 1).explode(unit, empty_as_null=True))
    runs = (multi.join(multi, on=["key", "sid"], suffix="_b").filter(pl.col(unit) < pl.col(f"{unit}_b"))
            .group_by(a=unit, b=f"{unit}_b").agg(shared=pl.len(), f=pl.col("sid").min(), l=pl.col("sid").max())
            .filter(pl.col("shared") >= MIN_SHARED))
    series = tr.select(u=pl.col(unit), sid="sid", close="close")
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
    """(symbol, sid, twin_cut): where the old name of a twin pair (the one that stops or jumps at the end of the
    shared run while the other carries on) must be cut because both symbols trade, with different bars, on the
    same sessions before the run (cut at its first shared bar) or after it (cut at its first bar after)."""
    old_a = (pl.col("a_state").is_in(["stops", "jumps"]) & (pl.col("b_state") == "smooth")) | (
        (pl.col("a_state") == "stops") & (pl.col("b_state") == "jumps"))
    old_b = (pl.col("b_state").is_in(["stops", "jumps"]) & (pl.col("a_state") == "smooth")) | (
        (pl.col("b_state") == "stops") & (pl.col("a_state") == "jumps"))
    pairs = pl.concat([runs.filter(old_a).select("f", "l", x="a", y="b"), runs.filter(old_b).select("f", "l", x="b", y="a")])
    s = tr.select("symbol", "sid")
    cuts = []
    for name, outside in (("twin_start", pl.col("sid") < pl.col("f")), ("twin_end", pl.col("sid") > pl.col("l"))):
        xs = pairs.join(s, left_on="x", right_on="symbol").filter(outside)
        ys = pairs.join(s, left_on="y", right_on="symbol").filter(outside)
        hit = xs.join(ys, on=["x", "y", "f", "l", "sid"], how="semi").select("x", "f", "l").unique()
        if name == "twin_start":
            cuts.append(hit.select(symbol="x", sid="f", twin_cut=pl.lit(name)))
        else:
            cuts.append(hit.join(s, left_on="x", right_on="symbol").filter(pl.col("sid") > pl.col("l"))
                        .group_by("x", "l").agg(pl.col("sid").min()).select(symbol="x", sid="sid", twin_cut=pl.lit(name)))
    return pl.concat(cuts).unique(["symbol", "sid"])


def _segments(tr: pl.DataFrame, symbol_runs: pl.DataFrame) -> pl.DataFrame:
    """Adds seg (symbol#n) and cut (the rule that started it, null for a symbol's first segment)."""
    tr = (tr.with_columns(gap=pl.col("sid").diff().over("symbol"),
                          move=(pl.col("close") / pl.col("close").shift().over("symbol")).log().abs())
          .join(_twin_cuts(tr, symbol_runs), on=["symbol", "sid"], how="left").sort("symbol", "sid"))
    cut = (pl.when(pl.col("gap") > LONG_GAP).then(pl.lit("long_gap"))
           .when((pl.col("gap") > MID_GAP) & (pl.col("move") > JUMP)).then(pl.lit("gap_jump"))
           .otherwise(pl.col("twin_cut")))
    return (tr.with_columns(cut=cut)
            .with_columns(seg=pl.col("symbol") + "#" + pl.col("cut").is_not_null().cum_sum().over("symbol").cast(pl.String))
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


def _components(nodes: list[str], edges: list[tuple[str, str]]) -> dict[str, str]:
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


def build(bars: pl.DataFrame, events: pl.DataFrame, sessions: list[date]) -> dict:
    """bars: one row per (symbol, day) with OHLCV, vwap, trade_count. events: staged corporate_events, latest
    version per id. sessions: every trading session covering the bars. Returns the identity tables and a report."""
    cal = pl.DataFrame({"day": sessions}, schema={"day": pl.Date}).with_columns(sid=pl.int_range(pl.len(), dtype=pl.Int64))
    last_sid = cal.height - 1
    tr = _traded(bars, cal)
    sym_runs = _twin_runs(tr, "symbol")
    tr = _segments(tr, sym_runs)
    segs = tr.group_by("seg").agg(symbol=pl.col("symbol").first(), first=pl.col("sid").min(), last=pl.col("sid").max(),
                                  first_close=pl.col("close").sort_by("sid").first(),
                                  last_close=pl.col("close").sort_by("sid").last(), n=pl.len())

    runs = _twin_runs(tr, "seg")
    smooth_twins = runs.filter((pl.col("a_state") == "smooth") & (pl.col("b_state") == "smooth"))
    twin_links = runs.join(smooth_twins, on=["a", "b"], how="anti").select("a", "b")
    ev_links = _event_links(segs, events, cal)
    comp = _components(segs["seg"].to_list(), list(twin_links.iter_rows()) + list(ev_links.iter_rows()))
    segs = segs.with_columns(comp=pl.col("seg").replace_strict(comp))

    # As-traded symbol per (security, session): the candidate segment that ends first, then the old symbol of a
    # later rename, then no bankruptcy Q suffix, then the shorter symbol, then alphabetical.
    renamed_later = (events.filter(pl.col("type").is_in(RENAME_TYPES)).sort("date").join_asof(cal.sort("day"), left_on="date", right_on="day",
                                                                                 strategy="forward")
                     .select(symbol="symbol", rename_sid="sid").drop_nulls().group_by("symbol").agg(pl.col("rename_sid").max()))
    cand = (tr.join(segs.select("seg", "comp", seg_last="last"), on="seg")
            .join(renamed_later, on="symbol", how="left")
            .with_columns(old_name=(pl.col("rename_sid") >= pl.col("sid")).fill_null(False),
                          q=pl.col("symbol").str.contains(r"^[A-Z]{4}Q$"), sym_len=pl.col("symbol").str.len_chars()))
    order = cand.sort(["comp", "sid", "seg_last", "old_name", "q", "sym_len", "symbol"],
                      descending=[False, False, False, True, False, False, False])
    per_day = order.group_by("comp", "sid", maintain_order=True).agg(
        pl.all().first(), n_cand=pl.len(), n_keys=pl.col("key").n_unique(),
        n_first=(pl.col("seg_last") == pl.col("seg_last").first()).sum(),
        fetch_symbol=pl.col("symbol").sort_by("seg_last", descending=True).first())
    per_day = per_day.with_columns(tie=pl.col("n_first") > 1, conflict=pl.col("n_keys") > 1)

    firsts = per_day.group_by("comp").agg(first_symbol=pl.col("symbol").sort_by("sid").first(),
                                          first_sid=pl.col("sid").min(), last_sid=pl.col("sid").max(),
                                          last_symbol=pl.col("symbol").sort_by("sid").last(),
                                          tie_days=pl.col("tie").sum(), conflict_days=pl.col("conflict").sum())
    firsts = firsts.join(cal.rename({"sid": "first_sid", "day": "first_session"}), on="first_sid") \
        .join(cal.rename({"sid": "last_sid", "day": "last_session"}), on="last_sid")
    firsts = firsts.with_columns(security_id=pl.struct("first_symbol", "first_session").map_elements(
        lambda r: _security_id(r["first_symbol"], r["first_session"]), return_dtype=pl.String))

    traded_days = per_day.join(firsts.select("comp", "security_id"), on="comp").select(
        "security_id", "sid", "symbol", "fetch_symbol", *PRICES, "volume", "trade_count", no_trade=pl.lit(False))
    span = firsts.select("security_id", sid=pl.int_ranges("first_sid", pl.col("last_sid") + 1)).explode("sid", empty_as_null=True)
    days = (span.join(traded_days, on=["security_id", "sid"], how="left")
            .with_columns(pl.col("no_trade").fill_null(True), pl.col("volume").fill_null(0), pl.col("trade_count").fill_null(0))
            .sort("security_id", "sid")
            .with_columns(pl.col("symbol", "fetch_symbol").forward_fill().over("security_id"))
            .join(cal, on="sid").rename({"day": "session"}).drop("sid")
            .select("security_id", "session", "symbol", "fetch_symbol", *PRICES, "volume", "trade_count", "no_trade"))

    traded_only = days.filter(~pl.col("no_trade"))
    symbols = (traded_only.with_columns(run=(pl.struct("symbol", "fetch_symbol") != pl.struct("symbol", "fetch_symbol").shift())
                                        .fill_null(True).cum_sum().over("security_id"))
               .group_by("security_id", "run").agg(pl.col("symbol", "fetch_symbol").first(),
                                                    valid_from=pl.col("session").min(), valid_to=pl.col("session").max())
               .drop("run").sort("security_id", "valid_from"))

    securities = _ends(firsts, events, symbols, last_sid)
    cut_rows = tr.with_columns(prev_seg=pl.col("seg").shift().over("symbol")).filter(pl.col("cut").str.starts_with("twin_"))
    cut_flags = (pl.concat([cut_rows.select("seg"), cut_rows.select(seg="prev_seg")])  # both sides of the cut
                 .join(segs.select("seg", "comp"), on="seg")
                 .join(firsts.select("comp", "security_id"), on="comp").select("security_id").unique())
    smooth_ids = (pl.concat([smooth_twins.select(seg="a"), smooth_twins.select(seg="b")]).join(segs.select("seg", "comp"), on="seg")
                  .join(firsts.select("comp", "security_id"), on="comp").select("security_id").unique())
    securities = securities.with_columns(flags=pl.concat_list(
        pl.when(pl.col("tie_days") > 0).then(pl.lit("tie")),
        pl.when(pl.col("conflict_days") > 0).then(pl.lit("conflict")),
        pl.when(pl.col("security_id").is_in(cut_flags["security_id"].implode())).then(pl.lit("twin_cut")),
        pl.when(pl.col("security_id").is_in(smooth_ids["security_id"].implode())).then(pl.lit("smooth_twin")),
    ).list.drop_nulls())

    dup = per_day.group_by("key").agg(n=pl.col("comp").n_unique()).filter(pl.col("n") > 1).height
    report = {
        "symbols": bars["symbol"].n_unique(), "traded_bars": tr.height, "segments": segs.height,
        "cuts": tr.group_by("cut").len().drop_nulls().sort("cut").to_dicts(),
        "twin_pairs_symbols": sym_runs.height, "twin_pairs_segments": runs.height,
        "smooth_twins_unlinked": smooth_twins.height, "event_links": ev_links.height,
        "securities": securities.height, "security_days": days.height, "traded_security_days": traded_only.height,
        "bars_merged_away": tr.height - traded_only.height, "tie_days": int(per_day["tie"].sum()),
        "conflict_days": int(per_day["conflict"].sum()), "bars_in_two_securities": dup,
        "end_reasons": securities.group_by("end_reason").len().sort("end_reason").to_dicts(),
        "flags": securities.explode("flags", empty_as_null=True).group_by("flags").len().drop_nulls().sort("flags").to_dicts(),
    }
    return {"security_days": days, "security_symbols": symbols, "securities": securities, "report": report}


def _ends(firsts: pl.DataFrame, events: pl.DataFrame, symbols: pl.DataFrame, last_sid: int) -> pl.DataFrame:
    """How each security ended: an ending action on its last symbol within END_WINDOW days of its last trade."""
    rank = {t: i for i, t in enumerate(END_TYPES)}
    match = (firsts.select("comp", "last_symbol", "last_session")
             .join(events.filter(pl.col("type").is_in(list(END_TYPES))).select("symbol", "type", "new_symbol", "date"),
                   left_on="last_symbol", right_on="symbol")
             .filter((pl.col("date") - pl.col("last_session")).dt.total_days().abs() <= END_WINDOW)
             .with_columns(r=pl.col("type").replace_strict(rank)).sort("r")
             .group_by("comp", maintain_order=True).agg(pl.col("type", "new_symbol", "date").first()))
    ends = firsts.join(match, on="comp", how="left")
    succ = (ends.filter(pl.col("new_symbol").is_not_null()).select("security_id", "new_symbol", "date")
            .join(symbols.select(succ_id="security_id", new_symbol="symbol", valid_from="valid_from", valid_to="valid_to"), on="new_symbol")
            .filter((pl.col("valid_to") >= pl.col("date")) & (pl.col("succ_id") != pl.col("security_id")))
            .sort("valid_from").group_by("security_id").agg(successor_id=pl.col("succ_id").first()))
    reason = (pl.when(pl.col("last_sid") >= last_sid - ACTIVE_WITHIN).then(pl.lit("active"))
              .when(pl.col("type").is_not_null()).then(pl.col("type").replace_strict(END_TYPES, default=None))
              .otherwise(pl.lit("delisted_unknown")))
    return (ends.with_columns(end_reason=reason).join(succ, on="security_id", how="left")
            .select("security_id", "first_symbol", "last_symbol", "first_session", "last_session", "end_reason",
                    "successor_id", "tie_days", "conflict_days"))


def load(data_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Latest version of every daily bar and every corporate event in staged."""
    bars = (pl.scan_parquet(sorted((data_dir / "staged" / "daily_bars").glob("ingest_date=*/*.parquet")))
            .with_columns(day=pl.col("bar_start").dt.convert_time_zone("America/New_York").dt.date())
            .sort("ingested_at").group_by("symbol", "day").last()
            .select("symbol", "day", *PRICES, "volume", "trade_count").collect())
    files = sorted((data_dir / "staged" / "corporate_events").glob("ingest_date=*/*.parquet"))
    events = (pl.concat([pl.scan_parquet(f) for f in files], how="diagonal_relaxed")
              .sort("ingested_at").group_by("id").last().collect())
    return bars, events


def run(data_dir: Path) -> dict:
    from pit.market_hours import calendar
    bars, events = load(data_dir)
    sessions = [s.date() for s in calendar().sessions_in_range(bars["day"].min(), bars["day"].max())]
    out = build(bars, events, sessions)
    root = data_dir / "curated" / "identity"
    for name in ("security_days", "security_symbols", "securities"):
        write_parquet(out[name], root / f"{name}.parquet")
    (root / "report.json").write_text(json.dumps(out["report"], indent=2, default=str), encoding="utf-8")
    return out["report"]


if __name__ == "__main__":
    print(json.dumps(run(Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "data"))), indent=2,
                     default=str))
