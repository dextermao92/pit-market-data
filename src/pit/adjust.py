"""Point-in-time price adjustment, computed at query time from unadjusted bars.

Only corporate actions known by `as_of` (ingested_at <= as_of, latest version per id) and already
effective (ex_date <= as_of's New York date) adjust anything. A bar on session day d is multiplied by the
product of the factors of actions with ex_date > d:
  split / stock dividend: price_factor (volume is divided by it)
  cash dividend:          1 - cash / close of the last session before ex_date (CRSP-style)
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

NY = "America/New_York"
PRICES = ("open", "high", "low", "close", "vwap")


def _day(col: str) -> pl.Expr:
    return pl.col(col).dt.convert_time_zone(NY).dt.date()


def session_closes(bars: pl.DataFrame) -> pl.DataFrame:
    """(symbol, day, close): last bar's close per symbol and session day."""
    return bars.sort("bar_start").group_by("symbol", day=_day("bar_start")).agg(pl.col("close").last())


def known_actions(actions: pl.DataFrame, as_of: datetime) -> pl.DataFrame:
    as_of_day = as_of.astimezone(ZoneInfo(NY)).date()
    return (actions.filter(pl.col("ingested_at") <= as_of)
            .sort("ingested_at").group_by("id").last()
            .filter(pl.col("ex_date") <= as_of_day))


def adjust(bars: pl.DataFrame, actions: pl.DataFrame, closes: pl.DataFrame, as_of: datetime,
           how: str = "all") -> pl.DataFrame:
    """bars: unadjusted bars. closes: session closes covering every dividend's previous session
    (usually session_closes() over all known bars). how: "split" or "all"."""
    acts = known_actions(actions, as_of)
    splits = acts.filter(pl.col("cash").is_null()).select("symbol", "ex_date", "price_factor", is_split=pl.lit(True))
    divs = (acts.filter(pl.col("cash").is_not_null()).sort("ex_date")
            .join_asof(closes.sort("day"), left_on="ex_date", right_on="day", by="symbol",
                       strategy="backward", allow_exact_matches=False, check_sortedness=False)
            # ponytail: no close before ex_date -> dividend ignored; backfill bars first if that matters.
            .select("symbol", "ex_date", price_factor=(1 - pl.col("cash") / pl.col("close")).fill_null(1.0),
                    is_split=pl.lit(False)))
    events = pl.concat([splits, divs]) if how == "all" else splits

    days = bars.select("symbol", day=_day("bar_start")).unique()
    factors = (days.join(events, on="symbol").filter(pl.col("ex_date") > pl.col("day"))
               .group_by("symbol", "day")
               .agg(price_adj=pl.col("price_factor").product(),
                    split_adj=pl.col("price_factor").filter(pl.col("is_split")).product()))
    return (bars.with_columns(day=_day("bar_start"))
            .join(factors, on=["symbol", "day"], how="left")
            .with_columns(pl.col("price_adj", "split_adj").fill_null(1.0))
            .with_columns(*(pl.col(c) * pl.col("price_adj") for c in PRICES),
                          volume=pl.col("volume") / pl.col("split_adj"))
            .drop("day"))
