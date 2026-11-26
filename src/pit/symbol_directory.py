"""Nasdaq Trader symbol directories: every security listed on a US exchange today, with its full name and an ETF
flag (Stage 2 P1b security type, docs/stage2/p1b-curate-design.md section 2).

nasdaqlisted.txt (Nasdaq listings) and otherlisted.txt (NYSE, NYSE American, NYSE Arca, Cboe BZX, IEX), pipe
delimited, rebuilt through the day, current only: there is no archive, so a daily fetch is what builds the
history from now on. Raw keeps both files as received; staged has one row per (file, symbol).
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import polars as pl

from pit.rules import apply
from pit.storage import TS, write_raw, write_staged

BASE = "https://www.nasdaqtrader.com/dynamic/SymDir/"
FILES = ("nasdaqlisted", "otherlisted")
EXCHANGES = {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca", "Z": "Cboe BZX", "V": "IEX"}
SCHEMA = {"ingested_at": TS, "file": pl.String, "symbol": pl.String, "name": pl.String, "exchange": pl.String,
          "etf": pl.Boolean, "test_issue": pl.Boolean, "market_category": pl.String, "financial_status": pl.String,
          "cqs_symbol": pl.String, "nasdaq_symbol": pl.String, "file_created": pl.String}


def run(now: datetime | None = None) -> Path:
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    files = {}
    with httpx.Client(base_url=BASE, timeout=60, headers={"User-Agent": "pit-market-data"}) as c:
        for name in FILES:
            r = c.get(f"{name}.txt")
            r.raise_for_status()
            files[name] = r.text
    payload = {"ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1), "files": files}
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "nasdaqtrader", "symbol_directory", payload, now)


def _rows(text: str) -> tuple[list[dict], str | None]:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    header = lines[0].split("|")
    created = None
    rows = []
    for ln in lines[1:]:
        if ln.startswith("File Creation Time"):
            created = ln.split("|")[0].split(":", 1)[1].strip()
            continue
        rows.append(dict(zip(header, (v.strip() for v in ln.split("|")), strict=False)))
    return rows, created


def to_frame(payload: dict) -> pl.DataFrame:
    at = datetime.fromisoformat(payload["ingested_at"])
    out = []
    for file, text in payload["files"].items():
        rows, created = _rows(text)
        for r in rows:
            if file == "nasdaqlisted":
                out.append({"symbol": r.get("Symbol"), "exchange": "Nasdaq", "market_category": r.get("Market Category"),
                            "financial_status": r.get("Financial Status"), "cqs_symbol": None,
                            "nasdaq_symbol": r.get("Symbol"), **_common(r)})
            else:
                out.append({"symbol": r.get("ACT Symbol"), "exchange": EXCHANGES.get(r.get("Exchange"), r.get("Exchange")),
                            "market_category": None, "financial_status": None, "cqs_symbol": r.get("CQS Symbol"),
                            "nasdaq_symbol": r.get("NASDAQ Symbol"), **_common(r)})
            out[-1] |= {"ingested_at": at, "file": file, "file_created": created}
    return pl.DataFrame(out, schema=SCHEMA)


def _common(r: dict) -> dict:
    return {"name": r.get("Security Name"), "etf": r.get("ETF") == "Y", "test_issue": r.get("Test Issue") == "Y"}


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(
        to_frame(json.loads(raw_path.read_text(encoding="utf-8"))),
        errors={"required_fields": pl.any_horizontal(pl.col(c).is_null() | (pl.col(c) == "") for c in ("symbol", "name"))},
        warns={"test_issue": pl.col("test_issue")},
    )
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run())
