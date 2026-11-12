"""Sample for the Databricks port: a few symbols over the last month, raw and staged.

Usage: uv run python scripts/export_dbx_sample.py [--data data] [--out sample_data/dbx] [--end 2026-10-29]

  raw/source=alpaca/ingest_date=D/equity_bars/<ingested_at>.json   same layout and payload as data/, bars kept
                                                                   for the sample symbols only (request untouched)
  staged/<dataset>.parquet                                         every version (all ingested_at) of the sample
                                                                   symbols; bars limited to the window

Only what was ingested by --end counts (eod keeps refetching old sessions), so the same --end gives the same output
on any later day. sample_data/ itself belongs to pit.sample (CI), hence the subfolder.
"""

import argparse
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import polars as pl

SYMBOLS = ("SPY", "QQQ", "AAPL", "NVDA", "TSM")
NY = "America/New_York"


def export_raw(data: Path, out: Path, start: date, end: date) -> int:
    n = 0
    for f in sorted((data / "raw" / "source=alpaca").glob("ingest_date=*/equity_bars/*.json")):
        if date.fromisoformat(f.parents[1].name.removeprefix("ingest_date=")) > end:
            continue
        payload = json.loads(f.read_text())
        if not start <= date.fromisoformat(payload["session"]) <= end:
            continue
        for key in ("pages", "prev_pages"):
            for page in payload.get(key, []):
                page["bars"] = {s: b for s, b in page["bars"].items() if s in SYMBOLS}
        dest = out / f.relative_to(data)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(payload))
        n += 1
    return n


def export_staged(data: Path, out: Path, start: date, end: date) -> dict[str, int]:
    rows = {}
    for dataset in ("equity_bars", "daily_bars", "assets", "corporate_actions"):
        df = (pl.scan_parquet(data / "staged" / dataset / "**" / "*.parquet")
              .filter(pl.col("symbol").is_in(SYMBOLS), pl.col("ingested_at").dt.date() <= end))
        if dataset == "equity_bars":
            df = df.filter(pl.col("bar_start").dt.convert_time_zone(NY).dt.date().is_between(start, end))
        df = df.sort("ingested_at", "symbol").collect()
        df.write_parquet(out / "staged" / f"{dataset}.parquet")
        rows[dataset] = df.height
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", type=Path, default=Path("data"))
    p.add_argument("--out", type=Path, default=Path("sample_data/dbx"))
    p.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    p.add_argument("--days", type=int, default=30)
    a = p.parse_args()
    start = a.end - timedelta(days=a.days)

    shutil.rmtree(a.out, ignore_errors=True)  # a rerun must not keep files from a different window
    (a.out / "staged").mkdir(parents=True)
    files = export_raw(a.data, a.out, start, a.end)
    rows = export_staged(a.data, a.out, start, a.end)
    size = sum(f.stat().st_size for f in a.out.rglob("*") if f.is_file())
    print(f"{start}..{a.end} {','.join(SYMBOLS)}: {files} raw files, {rows}, {size / 1e6:.1f} MB in {a.out}")


if __name__ == "__main__":
    main()
