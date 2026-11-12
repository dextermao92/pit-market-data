"""Point-in-time queries over staged data: "what did I know as of time T?"

Staged keeps every version of every row (one file per fetch, `ingested_at` = when we learned it), so a query
as of T is: only rows with ingested_at <= T, latest version per key, then adjust with what was known by T.
Two clocks: bar_start / ts is market time, as_of is knowledge time. Delta time travel is a third (table version).
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from pit.adjust import adjust, session_closes

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
