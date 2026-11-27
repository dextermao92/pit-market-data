"""Point-in-time queries over staged data: "what did I know as of time T?"

Staged keeps every version of every row (one file per fetch, `ingested_at` = when we learned it), so a query
as of T is: only rows with ingested_at <= T, latest version per key, then adjust with what was known by T.
Two clocks: bar_start / ts is market time, as_of is knowledge time. Delta time travel is a third (table version).
"""

import os
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from pit.adjust import adjust, adjust_daily, attach, session_closes

KEY = ("symbol", "bar_start")


def _data_dir(data_dir: Path | None) -> Path:
    return data_dir or Path(os.environ.get("DATA_DIR", "data"))


def _files(data_dir: Path, dataset: str, as_of: datetime) -> list[Path]:
    """Staged files fetched at or before as_of, oldest first.

    A staged file is named after its fetch time (UTC, fixed width), so names sort by time and files fetched after
    as_of are skipped without being opened. That is also what makes an as_of query reproducible: later fetches
    only ever add files with later names.
    """
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    cutoff = f"{as_of.astimezone(UTC):%Y%m%dT%H%M%S%fZ}"
    files = (data_dir / "staged" / dataset).glob("ingest_date=*/*.parquet")
    return sorted((f for f in files if f.stem <= cutoff), key=lambda f: f.stem)


def _scan(files: list[Path], as_of: datetime) -> pl.LazyFrame | None:
    if not files:
        return None
    # one scan per file: schemas drift a little over time (new columns, all-null columns), so concat relaxed
    frames = pl.concat([pl.scan_parquet(f) for f in files], how="diagonal_relaxed")
    return frames.filter(pl.col("ingested_at") <= as_of)  # the file name already says so; this is the guarantee


def get_bars(symbols: list[str], start: datetime, end: datetime, as_of: datetime, *,
             data_dir: Path | None = None, how: str = "all") -> pl.DataFrame:
    """5-min bars with start <= bar_start < end, as known at as_of, adjusted with the actions known at as_of.

    One row per (symbol, bar_start), sorted by both; `ingested_at` says which version was used.
    how: passed to adjust() ("all" or "split").
    """
    root = _data_dir(data_dir)
    bars = _scan(_files(root, "equity_bars", as_of), as_of)
    if bars is None:
        return pl.DataFrame()
    known = (bars.filter(pl.col("symbol").is_in(symbols))
             .sort("ingested_at").group_by(KEY, maintain_order=True).last()  # latest version known at as_of
             .collect())
    cols = known.columns
    if not known.height:
        return known

    actions = _scan(_files(root, "corporate_actions", as_of), as_of)
    actions = (actions.filter(pl.col("symbol").is_in(symbols)).collect() if actions is not None
               else pl.DataFrame(schema={"ingested_at": known["ingested_at"].dtype, "id": pl.String,
                                         "symbol": pl.String, "ex_date": pl.Date, "price_factor": pl.Float64,
                                         "cash": pl.Float64}))
    # closes before the window cut: a dividend needs the close of the session before its ex_date
    closes = session_closes(known)
    window = known.filter((pl.col("bar_start") >= start) & (pl.col("bar_start") < end))
    return adjust(window, actions, closes, as_of, how).select(cols).sort(KEY)


def get_chain(underlying: str, at: datetime, *, data_dir: Path | None = None) -> pl.DataFrame:
    """The latest option snapshot of `underlying` taken at or before `at`.

    Every row (all trading classes, expiries, strikes) of that one snapshot; empty if there is none.
    No as_of: a snapshot is never revised, so its ingested_at is both the market time and the knowledge time.
    Underlyings are staggered and a run may carry several of them, so walk back from `at` to the newest file
    that has this underlying. Note SPX's 0-2 DTE SPXW rows are only in the quarter-hour snapshots.
    """
    for f in reversed(_files(_data_dir(data_dir), "option_snapshots", at)):
        rows = pl.scan_parquet(f).filter(pl.col("underlying") == underlying).collect()
        if rows.height:
            latest = rows["ingested_at"].max()
            return rows.filter(pl.col("ingested_at") == latest)
    return pl.DataFrame()


# --- by security (Stage 2 P1b: curated/identity and universe_daily) ---

def get_universe(session: date, *, eligible_only: bool = True, data_dir: Path | None = None) -> pl.DataFrame:
    """Securities a strategy could trade at `session`'s open, from sessions before it (universe_daily, written
    by dbt): security_id, symbol (as traded the session before), type, flags, prev_close, adv20_usd, rank_adv.
    Sorted by rank_adv (most traded first)."""
    rows = pl.scan_parquet(_data_dir(data_dir) / "curated" / "universe_daily.parquet").filter(pl.col("session") == session)
    if eligible_only:
        rows = rows.filter("eligible")
    return rows.sort("rank_adv", "security_id", nulls_last=True).collect()


def get_security(symbol: str, session: date, *, data_dir: Path | None = None) -> str | None:
    """The security_id that traded under `symbol` on `session` (FB on 2020-01-02 is Meta; FB in 2025 a fund)."""
    periods = (pl.scan_parquet(_data_dir(data_dir) / "curated" / "identity" / "security_symbols.parquet")
               .filter((pl.col("symbol") == symbol) & (pl.col("valid_from") <= session) & (pl.col("valid_to") >= session))
               .collect())
    return periods["security_id"][0] if periods.height else None


def get_daily_bars(security_ids: list[str], start: date, end: date, *, as_of: datetime | None = None,
                   adjust: str = "all", traded_only: bool = True, data_dir: Path | None = None) -> pl.DataFrame:
    """Daily bars by security (identity's security_days), start <= session <= end, with the symbol each row
    traded under, adjusted by security with the corporate actions known at as_of ("all", "split" or "none").

    as_of (default now) also drops sessions after its New York date. Bars themselves are the latest version
    (identity is rebuilt, not versioned); actions are point-in-time. traded_only drops zero-volume filler days.
    """
    root = _data_dir(data_dir)
    as_of = as_of or datetime.now(UTC)
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    last = min(end, as_of.astimezone(ZoneInfo("America/New_York")).date())
    ident = root / "curated" / "identity"
    days = (pl.scan_parquet(sorted((ident / "security_days").glob("*.parquet")))
            .filter(pl.col("security_id").is_in(security_ids) & (pl.col("session") <= last))
            .collect())  # every day up to the window's end: a dividend needs the close before its ex_date
    if adjust != "none" and days.height:
        symbols = pl.read_parquet(ident / "security_symbols.parquet")
        actions = _scan(_files(root, "corporate_actions", as_of), as_of)
        held = symbols.filter(pl.col("security_id").is_in(security_ids))["symbol"].unique().to_list()
        if actions is not None:
            known = (actions.filter(pl.col("symbol").is_in(held))
                     .select("ingested_at", "id", "symbol", "ex_date", "price_factor", "cash").collect())
            days = adjust_daily(days, attach(known, symbols), as_of, adjust)
    window = days.filter(pl.col("session") >= start)
    if traded_only:
        window = window.filter(pl.col("volume") > 0)
    return window.sort("security_id", "session")
