"""Point-in-time queries over staged data: "what did I know as of time T?"

Staged keeps every version of every row (one file per fetch, `ingested_at` = when we learned it), so a query
as of T is: only rows with ingested_at <= T, latest version per key, then adjust with what was known by T.
Two clocks: bar_start / ts is market time, as_of is knowledge time. Delta time travel is a third (table version).
"""

import os
from datetime import datetime
from pathlib import Path

import polars as pl

from pit.adjust import adjust, session_closes  # noqa: F401  (you'll need both)


def _data_dir(data_dir: Path | None) -> Path:
    return data_dir or Path(os.environ.get("DATA_DIR", "data"))


def get_bars(symbols: list[str], start: datetime, end: datetime, as_of: datetime, *,
             data_dir: Path | None = None, how: str = "all") -> pl.DataFrame:
    """5-min bars with start <= bar_start < end, as known at as_of, adjusted with the actions known at as_of.

    One row per (symbol, bar_start), sorted by both; `ingested_at` says which version was used.
    how: passed to adjust() ("all" or "split").
    """
    # TODO(you), in order:
    # 1. scan staged/equity_bars/**/*.parquet; keep the symbols, and ingested_at <= as_of
    # 2. latest version per (symbol, bar_start): sort by ingested_at, then group_by(...).last()
    #    (or SQL: qualify row_number() over (partition by ... order by ingested_at desc) = 1, see bars_current.sql)
    # 3. scan staged/corporate_actions the same way, but just ingested_at <= as_of: adjust() picks versions itself
    # 4. closes for dividends: session_closes() over the known bars of those symbols *before* the window cut,
    #    since a dividend needs the close of the session before its ex_date, which may lie before `start`
    # 5. cut to [start, end), adjust(bars, actions, closes, as_of, how), sort
    raise NotImplementedError


def get_chain(underlying: str, at: datetime, *, data_dir: Path | None = None) -> pl.DataFrame:
    """The latest option snapshot of `underlying` taken at or before `at`.

    Every row (all trading classes, expiries, strikes) of that one snapshot; empty if there is none.
    """
    # TODO(you), after get_bars. Why no as_of here? A snapshot is never revised: its ingested_at is both when the
    # market looked like that and when we knew it, so the two clocks of get_bars collapse into one.
    # Underlyings are staggered (5 or 15 min), so filter on underlying before taking the latest ingested_at.
    raise NotImplementedError
