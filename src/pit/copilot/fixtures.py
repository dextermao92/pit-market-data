"""Data directories for the Copilot evals: a frozen copy of production data, and per-case copies with faults injected.

Production data/ is only ever read. `base` is a copy of what the MCP tools read (pipeline_runs and staged equity bars,
the SLO query's inputs), so real cases see the data as it was when the copy was made, however much data/ grows later.
A simulated case gets its own directory: the base hard-linked file by file (no extra space), plus one cloned session
day with faults applied. Cloned files are new files, so the base copy is never modified through a link either.

  uv run python -m pit.copilot.fixtures build [--src data]   copy data/ into evals/fixtures/base (refuses to overwrite)

Fault spec (a simulated case's `sim`):

  clone: {from: 2026-11-11, to: 2026-11-13}   copy one New York day's runs and equity bars to another date
  faults:                                      applied in order to the cloned day's runs, by ET start time [from, to)
    - {action: fail, datasets: [option_snapshots], from: "13:05", to: "16:00", error: "...", duration_s: 4,
       ibkr_error_codes: [2103]}
    - {action: drop, datasets: all, from: "09:00", to: "17:45"}           runs that never happened
    - {action: set, datasets: [option_snapshots], from: ..., to: ..., fields: {no_quote: 3000, rows: 3000}}
    - {action: shift, datasets: [option_snapshots], from: ..., to: ..., seconds: 240}   runs that started late
    - {action: drop_bars, from: "12:00", to: "12:45"}                     equity bars missing from staged
  gap_s is recomputed for the cloned day afterwards.
"""

import argparse
import glob
import json
import os
import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "evals" / "fixtures"
BASE = FIXTURES / "base"
COPIED = ("pipeline_runs", "staged/equity_bars")
NY = ZoneInfo("America/New_York")
RUN_TIMES = ("started_at", "finished_at")


def build_base(src: Path, dst: Path = BASE) -> dict:
    """Copy the tools' inputs from src (production data/) to dst. Read-only on src."""
    src, dst = Path(src).resolve(), Path(dst).resolve()
    if dst.exists():
        raise FileExistsError(f"{dst} exists; delete it first to rebuild the frozen copy")
    if dst.is_relative_to(src) or src.is_relative_to(dst):
        raise ValueError("source and destination overlap")
    for sub in COPIED:
        shutil.copytree(src / sub, dst / sub)
    manifest = {"copied_at": datetime.now().astimezone().isoformat(timespec="seconds"), "source": str(src),
                "files": {sub: len(list((dst / sub).rglob("*.parquet"))) for sub in COPIED}}
    (dst / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _link_tree(src: Path, dst: Path) -> None:
    for f in src.rglob("*"):
        if f.is_file():
            target = dst / f.relative_to(src)
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(f, target)
            except OSError:  # another volume, or links not supported
                shutil.copy2(f, target)


def _et_window(day: date, lo: str, hi: str) -> tuple[datetime, datetime]:
    return (datetime.combine(day, time.fromisoformat(lo), NY), datetime.combine(day, time.fromisoformat(hi), NY))


def _read_runs(root: Path) -> pl.DataFrame:
    files = glob.glob(str(root / "pipeline_runs" / "dataset=*" / "ingest_date=*" / "*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def _on_day(df: pl.DataFrame, col: str, day: date) -> pl.DataFrame:
    return df.filter(pl.col(col).dt.convert_time_zone("America/New_York").dt.date() == day)


def clone_day(runs: pl.DataFrame, src: date, dst: date) -> pl.DataFrame:
    """One ET day's runs moved to another date: times, run_ids and paths."""
    shift, a, b = dst - src, src.isoformat(), dst.isoformat()
    day = _on_day(runs, "started_at", src)
    text = [c for c in ("run_id", "raw_path", "staged_path") if c in day.columns]
    return day.with_columns(*[pl.col(c) + shift for c in RUN_TIMES],
                            *[pl.col(c).str.replace_all(a, b).str.replace_all(a.replace("-", ""), b.replace("-", ""))
                              for c in text])


def apply_faults(day: pl.DataFrame, faults: list[dict], on: date) -> pl.DataFrame:
    et = pl.col("started_at").dt.convert_time_zone("America/New_York")
    for f in faults:
        if f["action"] == "drop_bars":
            continue
        lo, hi = _et_window(on, f["from"], f["to"])
        hit = et.is_between(lo, hi, closed="left")
        if f.get("datasets", "all") != "all":
            hit = hit & pl.col("dataset").is_in(f["datasets"])
        if f["action"] == "drop":
            day = day.filter(~hit)
        elif f["action"] == "fail":
            nulls = [c for c in ("rows", "quarantined", "flagged", "two_sided", "ask_only", "bid_only", "no_quote",
                                 "crossed", "missing_bars", "raw_path", "staged_path", "cycle_s") if c in day.columns]
            day = day.with_columns(
                status=pl.when(hit).then(pl.lit("failed")).otherwise("status"),
                error=pl.when(hit).then(pl.lit(f.get("error"))).otherwise("error"),
                finished_at=pl.when(hit).then(pl.col("started_at") + timedelta(seconds=f.get("duration_s", 4)))
                .otherwise("finished_at"),
                ibkr_error_codes=pl.when(hit).then(pl.lit(f.get("ibkr_error_codes"), dtype=pl.List(pl.Int64)))
                .otherwise("ibkr_error_codes"),
                **{c: pl.when(hit).then(None).otherwise(c) for c in nulls})
        elif f["action"] == "shift":  # slots that started late, e.g. queued behind a hung run
            late = timedelta(seconds=f["seconds"])
            day = day.with_columns(*[pl.when(hit).then(pl.col(c) + late).otherwise(c).alias(c) for c in RUN_TIMES])
        elif f["action"] == "set":
            day = day.with_columns(**{k: pl.when(hit).then(pl.lit(v, dtype=day.schema.get(k))).otherwise(k)
                                      for k, v in f["fields"].items()})
        else:
            raise ValueError(f"unknown fault action {f['action']!r}")
    return day


def recompute_gaps(day: pl.DataFrame, before: pl.DataFrame) -> pl.DataFrame:
    """gap_s = start minus the previous successful run's start, per dataset, as pipeline.py records it."""
    out = []
    for (ds,), g in day.sort("started_at").group_by("dataset", maintain_order=True):
        prev_ok = before.filter((pl.col("dataset") == ds) & (pl.col("status") == "success"))["started_at"].max()
        gaps = []
        for r in g.iter_rows(named=True):
            gaps.append((r["started_at"] - prev_ok).total_seconds() if prev_ok else None)
            if r["status"] == "success":
                prev_ok = r["started_at"]
        out.append(g.with_columns(gap_s=pl.Series(gaps, dtype=pl.Float64)))
    return pl.concat(out, how="diagonal_relaxed") if out else day


def _write_runs(day: pl.DataFrame, root: Path) -> None:
    day = day.with_columns(_d=pl.col("started_at").dt.date())  # partitions are UTC dates
    for (ds, d), g in day.group_by("dataset", "_d"):
        out = root / "pipeline_runs" / f"dataset={ds}" / f"ingest_date={d}"
        out.mkdir(parents=True, exist_ok=True)
        g.drop("_d").write_parquet(out / "sim.parquet")


def _clone_bars(base: Path, root: Path, src: date, dst: date, gaps: list[dict]) -> None:
    files = glob.glob(str(base / "staged" / "equity_bars" / f"ingest_date={src}" / "*.parquet"))
    if not files:
        return
    shift = dst - src
    bars = _on_day(pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed"), "bar_start", src)
    bars = bars.with_columns(pl.col("ingested_at") + shift, pl.col("bar_start") + shift)
    for g in gaps:
        lo, hi = _et_window(dst, g["from"], g["to"])
        bars = bars.filter(~pl.col("bar_start").dt.convert_time_zone("America/New_York").is_between(lo, hi, closed="left"))
    out = root / "staged" / "equity_bars" / f"ingest_date={dst}"
    out.mkdir(parents=True, exist_ok=True)
    bars.write_parquet(out / "sim.parquet")


def build_case(case_id: str, sim: dict, base: Path = BASE, fixtures: Path = FIXTURES) -> Path:
    """A simulated case's data directory, rebuilt from the base each time."""
    if not base.exists():
        raise FileNotFoundError(f"{base} missing: run `uv run python -m pit.copilot.fixtures build` first")
    root = fixtures / case_id
    if root.exists():
        shutil.rmtree(root)
    _link_tree(base, root)
    src, dst = date.fromisoformat(str(sim["clone"]["from"])), date.fromisoformat(str(sim["clone"]["to"]))
    runs = _read_runs(base)
    if not _on_day(runs, "started_at", dst).is_empty():
        raise ValueError(f"{dst} already has runs in the base; clone onto a date without data")
    faults = sim.get("faults", [])
    day = recompute_gaps(apply_faults(clone_day(runs, src, dst), faults, dst),
                         runs.filter(pl.col("started_at").dt.convert_time_zone("America/New_York")
                                     < datetime.combine(dst, time(0), NY)))
    if not day.is_empty():
        _write_runs(day, root)
    _clone_bars(base, root, src, dst, [f for f in faults if f["action"] == "drop_bars"])
    return root


if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="python -m pit.copilot.fixtures")
    ap.add_argument("command", choices=["build"])
    ap.add_argument("--src", type=Path, default=ROOT / "data")
    a = ap.parse_args()
    print(json.dumps(build_base(a.src), indent=2))
