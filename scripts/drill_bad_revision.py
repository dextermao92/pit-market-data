"""SIMULATED drill: the vendor serves bad revisions of a session we already hold, then corrects them.

Works on a hard-linked copy of data/staged as it stood at 21:00 ET on 2026-11-10 (nothing under data/ is written
or modified; the drill only adds new files to the copy). It takes the real end-of-day fetch of 2026-11-10,
corrupts it four ways and stages it with the real staging rules as a refetch at 21:00 ET; runs the dbt build and
rules on that state; then stages a clean refetch at the next eod (eod refetches the last 10 sessions) and runs dbt
again. get_bars() is asked the same question as of before, during and after the bad window.

    uv run python scripts/drill_bad_revision.py [--work DIR] [--skip-dbt]
"""

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from pit.alpaca_bars import stage_file
from pit.query import get_bars

SOURCE = Path("data/raw/source=alpaca/ingest_date=2026-11-10/equity_bars/20261110T223108474260Z.json")  # real eod
SESSION = (datetime(2026, 11, 10, 14, 30, tzinfo=UTC), datetime(2026, 11, 10, 21, 0, tzinfo=UTC))
T_BAD = datetime(2026, 11, 11, 2, 0, tzinfo=UTC)    # 21:00 ET 11-10: a re-fetch picks up bad vendor data
T_FIX = datetime(2026, 11, 11, 22, 30, tzinfo=UTC)  # 17:30 ET 11-11: next eod refetches 11-10, vendor corrected
AS_OF = {"before": datetime(2026, 11, 11, 1, 0, tzinfo=UTC), "bad": datetime(2026, 11, 11, 3, 0, tzinfo=UTC),
         "fixed": datetime(2026, 11, 11, 23, 0, tzinfo=UTC)}
SYMBOLS = ["AAPL", "MSFT", "NVDA", "SPY"]
FAULTS = {
    "AAPL": "every price halved, volume doubled: looks like a 2:1 split nobody announced",
    "MSFT": "one bar (13:00 ET) with high below low",
    "NVDA": "every price 0.4% too high: plausible, no single bar looks wrong",
    "SPY": "one bar (15:00 ET) closes 3% low on 1% of normal volume: a bad tick",
}


def link_tree(src: Path, dst: Path, cutoff: datetime) -> int:
    """Hard-link the staged files fetched at or before cutoff (file names are fetch times, UTC)."""
    stem, n = f"{cutoff:%Y%m%dT%H%M%S%fZ}", 0
    for f in src.rglob("*.parquet"):
        if f.stem > stem:
            continue
        out = dst / f.relative_to(src)
        out.parent.mkdir(parents=True, exist_ok=True)
        os.link(f, out)
        n += 1
    return n


def corrupt(payload: dict) -> dict:
    p = copy.deepcopy(payload)
    for page in p["pages"]:
        for sym, bars in (page.get("bars") or {}).items():
            for b in bars:
                if sym == "AAPL":
                    for k in ("o", "h", "l", "c", "vw"):
                        b[k] = round(b[k] / 2, 4)
                    b["v"] *= 2
                elif sym == "NVDA":
                    for k in ("o", "h", "l", "c", "vw"):
                        b[k] = round(b[k] * 1.004, 4)
                elif sym == "MSFT" and b["t"] == "2026-11-10T18:00:00Z":
                    b["h"] = round(b["l"] * 0.99, 4)
                elif sym == "SPY" and b["t"] == "2026-11-10T20:00:00Z":
                    b["c"] = b["l"] = round(b["c"] * 0.97, 4)
                    b["v"] = b["v"] // 100
    return p


def write_raw(work: Path, payload: dict, at: datetime) -> Path:
    payload = payload | {"ingested_at": at.isoformat()}
    path = (work / "raw/source=alpaca" / f"ingest_date={at:%Y-%m-%d}" / "equity_bars"
            / f"{at:%Y%m%dT%H%M%S%fZ}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


def stage(work: Path, payload: dict, at: datetime) -> dict:
    _, staged, quarantined = stage_file(write_raw(work, payload, at))
    staged = staged.filter(pl.col("symbol").is_in(SYMBOLS))
    flagged = (staged.filter(pl.col("flags").list.len() > 0).explode("flags", empty_as_null=False)
               .group_by("symbol", "flags").len().sort("symbol").rows())
    return {"staged": staged.height, "quarantined": quarantined.select("symbol", "bar_start", "rules").rows(),
            "flagged": flagged}


def queries(work: Path) -> dict:
    out = {name: get_bars(SYMBOLS, *SESSION, as_of, data_dir=work) for name, as_of in AS_OF.items()}
    base = out["before"]
    report = {}
    for name, df in out.items():
        j = df.join(base, on=["symbol", "bar_start"], suffix="_before")
        report[name] = {
            "bars": df.height,
            "changed_vs_before": dict(j.filter(pl.col("close") != pl.col("close_before"))
                                      .group_by("symbol").len().sort("symbol").rows()),
            "flagged": dict(df.filter(pl.col("flags").list.len() > 0).group_by("symbol").len().sort("symbol").rows()),
            "aapl_mean_close": round(df.filter(pl.col("symbol") == "AAPL")["close"].mean(), 2),
            "msft_13et_close": df.filter((pl.col("symbol") == "MSFT")
                                         & (pl.col("bar_start") == datetime(2026, 11, 10, 18, tzinfo=UTC)))
                                 .select("close", "ingested_at").rows(),
        }
    reproducible = get_bars(SYMBOLS, *SESSION, AS_OF["bad"], data_dir=work).equals(out["bad"])
    return report | {"bad_as_of_reproducible_after_fix": reproducible}


def dbt_rules(work: Path) -> dict:
    """dbt build on the copy as it stands (bad version latest); rule failures stored and read back."""
    env = os.environ | {"DATA_DIR": str(work)}
    cmd = ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt", "--store-failures",
           "--target-path", str(work / "dbt_target"), "--log-path", str(work / "dbt_logs")]
    run = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)  # failing rules are the point
    summary = [line for line in run.stdout.splitlines() if "Done. PASS=" in line]
    con = duckdb.connect(str(work / "curated.duckdb"), read_only=True)
    out = {"dbt": summary[-1].split("Done. ")[-1] if summary else run.stdout[-500:]}
    for rule in ("rule14_daily_reconciliation", "rule15_split_jump_without_action", "rule16_cross_source_spot"):
        tables = [r[0] for r in con.execute(
            "select table_schema || '.' || table_name from information_schema.tables where table_name = ?",
            [rule]).fetchall()]
        if not tables:
            out[rule] = "no failures table (rule passed or did not run)"
            continue
        sym = "underlying" if rule.startswith("rule16") else "symbol"
        cur = con.execute(f"select * from {tables[0]}")  # no pyarrow here, so not .pl()
        rows = pl.DataFrame(cur.fetchall(), schema=[d[0] for d in cur.description], orient="row")
        if not rows.height:
            out[rule] = {"all_rows": 0, "on_2026_11_10": {}}
            continue
        day = (rows.filter(pl.col("day") == datetime(2026, 11, 10).date()) if "day" in rows.columns
               else rows.filter(pl.col("spot_time").dt.convert_time_zone("America/New_York").dt.date()
                                == datetime(2026, 11, 10).date()))
        out[rule] = {"all_rows": rows.height, "on_2026_11_10": dict(day.group_by(sym).len().sort(sym).rows())}
    con.close()
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", type=Path, default=None, help="scratch dir (default: a new temp dir)")
    ap.add_argument("--skip-dbt", action="store_true")
    args = ap.parse_args(argv)
    # same volume as data/, so the copy can be hard links
    work = args.work or Path(tempfile.mkdtemp(prefix="pit-drill-", dir="."))
    if work.exists() and any(work.iterdir()):
        sys.exit(f"{work} is not empty")
    linked = sum(link_tree(Path("data/staged") / ds, work / "staged" / ds, T_BAD)
                 for ds in ("equity_bars", "daily_bars", "corporate_actions", "option_snapshots", "assets"))
    payload = json.loads(SOURCE.read_text())
    result = {"simulated": True, "work": str(work), "linked_files": linked, "faults": FAULTS,
              "stage_bad": stage(work, corrupt(payload), T_BAD)}
    if not args.skip_dbt:
        result["dbt_on_bad_state"] = dbt_rules(work)
    result["stage_fix"] = stage(work, payload, T_FIX)
    if not args.skip_dbt:
        result["dbt_after_fix"] = dbt_rules(work)
    result["get_bars"] = queries(work)
    print(json.dumps(result, indent=2, default=str))
    if args.work is None:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
