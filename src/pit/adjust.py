"""Point-in-time price adjustment, computed at query time from unadjusted bars.

Only corporate actions known by `as_of` (ingested_at <= as_of, latest version per id) and already
effective (ex_date <= as_of's New York date) adjust anything. A bar on session day d is multiplied by the
product of the factors of actions with ex_date > d:
  split / stock dividend: price_factor (volume is divided by it)
  cash dividend:          1 - cash / close of the last session before ex_date (CRSP-style)

Two keys: `adjust` works on intraday bars by symbol; `adjust_daily` works on identity's security_days by
security, after `attach` has given each action the security that traded under its symbol on its date. By
security is what makes FB's 2016 bars carry META's later actions, and keeps an old holder's dividends off a
reused ticker's new holder.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

NY = "America/New_York"
PRICES = ("open", "high", "low", "close", "vwap")
ATTACH_WITHIN = 5  # calendar days between an action's ex_date and its symbol's period (renames on the ex_date)


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


def _factors(acts: pl.DataFrame, closes: pl.DataFrame, days: pl.DataFrame, key: str, how: str) -> pl.DataFrame:
    """(key, day, price_adj, split_adj) for the (key, day) pairs in `days`."""
    acts = acts.with_columns(pl.col("price_factor", "cash").cast(pl.Float64))  # an all-null column reads as Null
    splits = acts.filter(pl.col("cash").is_null()).select(key, "ex_date", "price_factor", is_split=pl.lit(True))
    divs = (acts.filter(pl.col("cash").is_not_null()).sort("ex_date")
            .join_asof(closes.sort("day"), left_on="ex_date", right_on="day", by=key,
                       strategy="backward", allow_exact_matches=False, check_sortedness=False)
            # ponytail: no close before ex_date -> dividend ignored; backfill bars first if that matters.
            .select(key, "ex_date", price_factor=(1 - pl.col("cash") / pl.col("close")).fill_null(1.0),
                    is_split=pl.lit(False)))
    events = pl.concat([splits, divs]) if how == "all" else splits
    return (days.join(events, on=key).filter(pl.col("ex_date") > pl.col("day"))
            .group_by(key, "day")
            .agg(price_adj=pl.col("price_factor").product(),
                 split_adj=pl.col("price_factor").filter(pl.col("is_split")).product()))


def _apply(df: pl.DataFrame, factors: pl.DataFrame, key: str) -> pl.DataFrame:
    return (df.join(factors, on=[key, "day"], how="left")
            .with_columns(pl.col("price_adj", "split_adj").fill_null(1.0))
            .with_columns(*(pl.col(c) * pl.col("price_adj") for c in PRICES),
                          volume=pl.col("volume") / pl.col("split_adj")))


def adjust(bars: pl.DataFrame, actions: pl.DataFrame, closes: pl.DataFrame, as_of: datetime,
           how: str = "all") -> pl.DataFrame:
    """bars: unadjusted bars. closes: session closes covering every dividend's previous session
    (usually session_closes() over all known bars). how: "split" or "all"."""
    with_day = bars.with_columns(day=_day("bar_start"))
    factors = _factors(known_actions(actions, as_of), closes, with_day.select("symbol", "day").unique(), "symbol", how)
    return _apply(with_day, factors, "symbol").drop("day", "price_adj", "split_adj")


def attach(actions: pl.DataFrame, symbols: pl.DataFrame) -> pl.DataFrame:
    """Actions with the security_id that traded under their symbol around their ex_date (identity's
    security_symbols periods, within ATTACH_WITHIN days); null when no security did (OTC, CUSIP-like symbols)."""
    ids = actions.select("id", "symbol", "ex_date").unique()
    dist = (pl.when(pl.col("ex_date") < pl.col("valid_from")).then(pl.col("valid_from") - pl.col("ex_date"))
            .when(pl.col("ex_date") > pl.col("valid_to")).then(pl.col("ex_date") - pl.col("valid_to"))
            .otherwise(pl.duration(days=0))).dt.total_days()
    best = (ids.join(symbols.select("security_id", "symbol", "valid_from", "valid_to"), on="symbol")
            .with_columns(dist=dist).filter(pl.col("dist") <= ATTACH_WITHIN)
            .sort("dist", pl.col("valid_from"), descending=[False, True])
            .group_by("id", "symbol", "ex_date", maintain_order=True).agg(pl.col("security_id").first()))
    return actions.join(best, on=["id", "symbol", "ex_date"], how="left")


def adjust_daily(days: pl.DataFrame, actions: pl.DataFrame, as_of: datetime, how: str = "all") -> pl.DataFrame:
    """days: identity's security_days rows (security_id, session, OHLC, vwap, volume, no_trade), every traded
    day before the window included so dividends find their previous close. actions: attach()'s output."""
    acts = known_actions(actions.filter(pl.col("security_id").is_not_null()), as_of)
    with_day = days.with_columns(day=pl.col("session"))
    closes = with_day.filter(pl.col("volume") > 0).select("security_id", "day", "close")
    factors = _factors(acts, closes, with_day.select("security_id", "day").unique(), "security_id", how)
    return _apply(with_day, factors, "security_id").drop("day", "price_adj", "split_adj")


def split_jumps(days: pl.DataFrame, actions: pl.DataFrame, ratio: float = 1.8) -> pl.DataFrame:
    """Rule 15 per security: traded close-to-close moves of `ratio`x or more (either way) with no split
    attached within 3 days. A warning to review, not a fix: a real crash or a missed split look the same.
    split_like: the move is within 2% of a whole ratio (2, 3, 10, 1/2 ...), the shape of a missed split. On
    2026-11-27 data, liquid eligible securities: 3,052 jumps, 482 split-like (UHAL's 1:10 stock dividend, UWTI's
    reverse split, but also spin-offs and crashes that happen to land near half)."""
    traded = (days.filter(pl.col("volume") > 0).sort("security_id", "session")
              .with_columns(prev=pl.col("close").shift().over("security_id"),
                            prev_session=pl.col("session").shift().over("security_id"))
              .filter(pl.col("prev").is_not_null())
              .with_columns(move=pl.col("close") / pl.col("prev"))
              .filter((pl.col("move") >= ratio) | (pl.col("move") <= 1 / ratio)))
    splits = actions.filter(pl.col("cash").is_null() & pl.col("security_id").is_not_null()).select(
        "security_id", "ex_date").unique()
    near = (traded.join(splits, on="security_id")
            .filter(pl.col("ex_date").is_between(pl.col("prev_session"), pl.col("session") + timedelta(days=3)))
            .select("security_id", "session").unique())
    whole = pl.when(pl.col("move") >= 1).then(pl.col("move")).otherwise(1 / pl.col("move"))
    return (traded.join(near, on=["security_id", "session"], how="anti")
            .with_columns(split_like=(whole.round(0) >= 2) & ((whole - whole.round(0)).abs() <= 0.02 * whole.round(0)))
            .select("security_id", "session", "symbol", "prev", "close", "move", "split_like"))
