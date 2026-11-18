"""SLIs of ops/slo.md over the last N sessions: runs ops/slo.sql against data/.

Usage: python -m pit.slo [--end YYYY-MM-DD] [--sessions 20] [--data-dir data]
"""

import argparse
import tomllib
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

from pit.market_hours import calendar, recent_sessions

ROOT = Path(__file__).resolve().parents[2]


def sessions(end: date | None, n: int) -> list[tuple[date, datetime, datetime]]:
    """The last n sessions closed by `end` (default: now): (session, open, close), open / close in UTC."""
    cal = calendar()
    now = datetime.now(timezone.utc) if end is None else cal.session_close(pd.Timestamp(end)).to_pydatetime()
    return [(d, cal.session_open(pd.Timestamp(d)).to_pydatetime(), cal.session_close(pd.Timestamp(d)).to_pydatetime())
            for d in recent_sessions(now, n)]


def compute(data_dir: Path, session_rows: list, n_symbols: int) -> pl.DataFrame:
    con = duckdb.connect()
    con.execute("set TimeZone = 'UTC'")
    con.execute("create table sessions (session date, open timestamptz, close timestamptz)")
    con.executemany("insert into sessions values (?, ?, ?)", session_rows)
    con.execute("set variable data_dir = ?", [str(data_dir)])
    con.execute("set variable n_symbols = ?", [n_symbols])
    rel = con.sql((ROOT / "ops" / "slo.sql").read_text(encoding="utf-8"))
    return pl.DataFrame(rel.fetchall(), schema=rel.columns, orient="row")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", type=date.fromisoformat)
    ap.add_argument("--sessions", type=int, default=20)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    a = ap.parse_args()
    n_symbols = len(tomllib.loads((ROOT / "config" / "alpaca_bars.toml").read_text(encoding="utf-8"))["alpaca"]["symbols"])
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=160):
        print(compute(a.data_dir, sessions(a.end, a.sessions), n_symbols))


if __name__ == "__main__":
    main()
