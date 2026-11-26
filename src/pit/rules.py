"""Row quality rules.

error -> row goes to quarantine (kept as evidence, never deleted)
warn  -> row stays in staged with the rule name in `flags`
No rule rejects on move size alone: a 5% index day or a +200% option is data.
Rules only look at one raw file (plus context stored in it), so staging stays a pure function of raw.
"""

from datetime import datetime, timedelta

import polars as pl

NY = "America/New_York"
SPLIT_RATIOS = (2, 3, 4, 5, 8, 10, 15, 20, 25, 50)
SPLIT_TOLERANCE = 0.03
# ponytail: fixed bad-tick thresholds; make them per-symbol (e.g. multiples of typical 5-min move) if they misfire.
BAD_TICK_MOVE = 0.01        # bar close at least 1% away from both neighbours...
BAD_TICK_VOLUME = 0.2       # ...on less than 20% of the symbol's median bar volume


def apply(df: pl.DataFrame, errors: dict[str, pl.Expr], warns: dict[str, pl.Expr]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Returns (staged rows with `flags`, quarantined rows with `rules` and `flags`)."""
    def hits(rules):
        return pl.concat_list([pl.when(e.fill_null(False)).then(pl.lit(n)) for n, e in rules.items()]).list.drop_nulls()

    df = df.with_columns(rules=hits(errors), flags=hits(warns))
    failed = pl.col("rules").list.len() > 0
    return df.filter(~failed).drop("rules"), df.filter(failed)


def _required(*cols: str) -> pl.Expr:
    return pl.any_horizontal(pl.col(c).is_null() for c in cols)


def check_options(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    snapshot_day = pl.col("ingested_at").dt.convert_time_zone(NY).dt.date()
    return apply(df, errors={
        "required_fields": _required("con_id", "expiry", "strike", "right"),
        "duplicate_contract": pl.col("con_id").is_duplicated(),
        "crossed_quote": pl.col("quote_status") == "crossed",
        "expired_contract": pl.col("expiry") < snapshot_day,
        "negative_iv": pl.col("iv") < 0,
    }, warns={
        "non_standard_contract": pl.col("multiplier") != 100,
        "missing_iv": (pl.col("quote_status") == "two_sided") & pl.col("iv").is_null(),
    })


def _near_split_ratio(x: pl.Expr) -> pl.Expr:
    # forward k:1 -> price x ~ 1/k; reverse 1:k -> x ~ k
    return pl.any_horizontal(
        ((x * k - 1).abs() < SPLIT_TOLERANCE) | ((x / k - 1).abs() < SPLIT_TOLERANCE) for k in SPLIT_RATIOS
    )


def check_bars(df: pl.DataFrame, prev_close: pl.DataFrame, intraday: bool = True) -> tuple[pl.DataFrame, pl.DataFrame]:
    """prev_close: (symbol, prev_close) = close of each symbol's last bar in the previous session.
    The warn rules are about intraday bars (one session per fetch); daily bars get the error rules only."""
    df = df.sort("symbol", "bar_start").join(prev_close, on="symbol", how="left")
    c, by = pl.col("close"), "symbol"
    up, down = c / c.shift(1).over(by) - 1, c.shift(-1).over(by) / c - 1
    overnight = pl.col("open").first().over(by) / pl.col("prev_close")
    still = c.last().over(by) / pl.col("prev_close")
    staged, quarantined = apply(df, errors={
        "required_fields": _required("symbol", "bar_start", "open", "high", "low", "close", "volume"),
        "duplicate_bar": pl.struct("symbol", "bar_start").is_duplicated(),
        "ohlc_invariant": (pl.col("low") > pl.min_horizontal("open", "close"))
                          | (pl.col("high") < pl.max_horizontal("open", "close"))
                          | (pl.col("volume") < 0),
    }, warns={
        # Corporate-action matching happens downstream, where actions and bars meet (curated layer).
        "overnight_split_ratio_jump": _near_split_ratio(overnight) & _near_split_ratio(still) & intraday,
        "bad_tick": (up.abs() > BAD_TICK_MOVE) & (down.abs() > BAD_TICK_MOVE) & (up.sign() != down.sign())
                    & (pl.col("volume") < BAD_TICK_VOLUME * pl.col("volume").median().over(by)) & intraday,
    })
    return staged.drop("prev_close"), quarantined.drop("prev_close")


def check_corporate_actions(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return apply(df, errors={
        "required_fields": _required("id", "symbol", "type", "ex_date"),
        "non_positive_ratio": pl.col("price_factor") <= 0,
        "negative_cash": pl.col("cash") < 0,
    }, warns={
        "special_dividend": pl.col("special"),
    })


def check_corporate_events(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return apply(df, errors={"required_fields": _required("id", "type", "symbol", "date")}, warns={
        "cusip_symbol": pl.col("symbol").str.contains(r"^[0-9A-Z]{9}$") & pl.col("symbol").str.contains(r"[0-9]"),
    })


def missing_bars(df: pl.DataFrame, symbols: list[str], start: datetime, end: datetime, bar: timedelta) -> int:
    """Partition check: expected (symbol, bar_start) slots in [start, end] minus those present (staged or quarantined).
    A warning, not an error: halts and no-trade intervals leave real gaps."""
    slots = int((end - start) / bar) + 1
    return len(symbols) * slots - df.select("symbol", "bar_start").unique().height
