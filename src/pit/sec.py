"""SEC files for security type and identity (Stage 2 P1b, docs/stage2/p1b-curate-design.md section 2b).

- `sec_ftd`: fail-to-deliver half-months (`cnsfailsYYYYMM{a,b}.zip`): settlement date, CUSIP, symbol and
  description of nearly every listed security that had a fail, so a period name and CUSIP for most delisted ones;
- `sec_fund_classes`: the yearly investment company series/class CSV: fund class tickers that existed that year;
- `sec_tickers`: `company_tickers_exchange.json`, today's ticker -> CIK.

SEC asks automated clients for a User-Agent with a contact address. It comes from SEC_USER_AGENT (.env) and
nothing is sent without it; it is never written anywhere. At most 5 requests a second (SEC allows 10).
Raw keeps each file exactly as served (raw/source=sec/.../<ingested_at>__<file name>); file lists come from SEC's
pages, because the files sit under several paths and the 2016 fund file has no year in its name.
"""

import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import polars as pl

from pit.market_hours import collection_window
from pit.rules import apply
from pit.storage import TS, write_raw_bytes, write_staged

BASE = "https://www.sec.gov"
FTD_PAGE = "/data-research/sec-markets-data/fails-deliver-data"
FUND_PAGE = "/open/datasets-investment_company.html"
TICKERS = "/files/company_tickers_exchange.json"
FTD_SINCE = "201601a"         # daily bars start in 2016
FUND_SINCE = 2016
MIN_INTERVAL = 0.2            # 5 requests a second
RETRIES = 5
STOP_MARGIN = timedelta(minutes=5)

FTD_SCHEMA = {"ingested_at": TS, "period": pl.String, "symbol": pl.String, "cusip": pl.String,
              "description": pl.String, "first_date": pl.Date, "last_date": pl.Date, "days": pl.Int64,
              "last_price": pl.Float64}
FUND_SCHEMA = {"ingested_at": TS, "year": pl.Int64, "cik": pl.String, "entity_name": pl.String, "org_type": pl.String,
               "series_id": pl.String, "series_name": pl.String, "class_id": pl.String, "class_name": pl.String,
               "ticker": pl.String, "file_number": pl.String}
TICKER_SCHEMA = {"ingested_at": TS, "cik": pl.String, "name": pl.String, "ticker": pl.String, "exchange": pl.String}
FUND_COLUMNS = {  # both header styles SEC has used -> staged name
    "Reporting File Number": "file_number", "rep_file_num": "file_number",
    "CIK Number": "cik", "CIK": "cik", "Entity Name": "entity_name", "entity_name": "entity_name",
    "Entity Org Type": "org_type", "entity_org_type": "org_type", "Series ID": "series_id", "series_id": "series_id",
    "Series Name": "series_name", "series_name": "series_name", "Class ID": "class_id", "class_id": "class_id",
    "Class Name": "class_name", "class_name": "class_name", "Class Ticker": "ticker", "class_ticker_symbol": "ticker",
}

_last_request = 0.0


def client() -> httpx.Client:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua:
        raise RuntimeError("SEC_USER_AGENT is not set: SEC asks for a User-Agent with a contact address (.env)")
    return httpx.Client(base_url=BASE, headers={"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}, timeout=120,
                        follow_redirects=True)


def get(c: httpx.Client, url: str) -> httpx.Response:
    """Paced GET; 429/503 are retried with backoff (Retry-After when given)."""
    global _last_request
    for attempt in range(RETRIES + 1):
        wait = _last_request + MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
        r = c.get(url)
        if r.status_code not in (429, 503) or attempt == RETRIES:
            r.raise_for_status()
            return r
        retry_after = r.headers.get("retry-after", "")
        time.sleep(float(retry_after) if retry_after.isdigit() else min(60, 2 ** (attempt + 1)))
    raise AssertionError("unreachable")


def ftd_links(html: str) -> dict[str, str]:
    """period ('201601a') -> path, from the fail-to-deliver page."""
    out = {}
    for path, ym, half in re.findall(r'href="([^"]*cnsfails(\d{6})([ab])\.zip)"', html):
        out.setdefault(ym + half, path)
    return out


def fund_links(html: str) -> dict[int, str]:
    """year -> path of the series/class CSV, the year read from the link text (the 2016 file name has none)."""
    out = {}
    for path, text in re.findall(r'<a[^>]+href="([^"]*series[^"]*class[^"]*\.csv)"[^>]*>(.*?)</a>', html, re.S | re.I):
        years = re.findall(r"\b(20\d\d)\b", re.sub(r"<[^>]+>", " ", text)) or re.findall(r"(20\d\d)", path.rsplit("/", 1)[-1])
        if years:
            out.setdefault(int(years[0]), path)
    return out


def _fetched(data_dir: Path, dataset: str) -> set[str]:
    """SEC file names already in raw (the part after <ingested_at>__)."""
    return {f.name.split("__", 1)[1] for f in data_dir.glob(f"raw/source=sec/ingest_date=*/{dataset}/*__*")
            if f.suffix != ".tmp"}


def _stop_by(now: datetime) -> datetime:
    return collection_window(now)[0] - STOP_MARGIN


def fetch_ftd(data_dir: Path, since: str = FTD_SINCE, now: datetime | None = None) -> dict:
    """Every fail-to-deliver half-month from `since` not already in raw; stops before the collection window and
    resumes on the next run."""
    stop_by = _stop_by(now or datetime.now(timezone.utc))
    stats = {"files": 0, "rows": 0, "quarantined": 0, "flagged": 0, "stopped": False, "raw_path": None,
             "staged_path": None}
    with client() as c:
        links = ftd_links(get(c, FTD_PAGE).text)
        have = _fetched(data_dir, "sec_ftd")
        for period in sorted(p for p in links if p >= since):
            name = links[period].rsplit("/", 1)[-1]
            if name in have:
                continue
            if datetime.now(timezone.utc) >= stop_by:
                stats["stopped"] = True
                break
            raw = write_raw_bytes(data_dir, "sec", "sec_ftd", name, get(c, links[period]).content,
                                  datetime.now(timezone.utc))
            _count(stats, raw, *stage_ftd(raw))
    return stats


def fetch_fund_classes(data_dir: Path, since: int = FUND_SINCE) -> dict:
    """Each year's series/class CSV from `since` not already in raw (raw name: <year>__<SEC file name>)."""
    stats = {"files": 0, "rows": 0, "quarantined": 0, "flagged": 0, "stopped": False, "raw_path": None,
             "staged_path": None}
    with client() as c:
        links = fund_links(get(c, FUND_PAGE).text)
        have = _fetched(data_dir, "sec_fund_classes")
        for year in sorted(y for y in links if y >= since):
            name = f"{year}__{links[year].rsplit('/', 1)[-1]}"
            if name in have:
                continue
            raw = write_raw_bytes(data_dir, "sec", "sec_fund_classes", name, get(c, links[year]).content,
                                  datetime.now(timezone.utc))
            _count(stats, raw, *stage_fund_classes(raw))
    return stats


def fetch_tickers(data_dir: Path) -> dict:
    stats = {"files": 0, "rows": 0, "quarantined": 0, "flagged": 0, "stopped": False, "raw_path": None,
             "staged_path": None}
    with client() as c:
        raw = write_raw_bytes(data_dir, "sec", "sec_tickers", "company_tickers_exchange.json", get(c, TICKERS).content,
                              datetime.now(timezone.utc))
    _count(stats, raw, *stage_tickers(raw))
    return stats


def _count(stats: dict, raw: Path, staged_path: Path, staged: pl.DataFrame, quarantined: pl.DataFrame) -> None:
    stats["files"] += 1
    stats["rows"] += staged.height
    stats["quarantined"] += quarantined.height
    stats["flagged"] += staged.filter(pl.col("flags").list.len() > 0).height
    stats["raw_path"], stats["staged_path"] = str(raw), str(staged_path)


def _ingested_at(raw_path: Path) -> datetime:
    return datetime.strptime(raw_path.name.split("__", 1)[0], "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)


def cusip_ok(cusip: str | None) -> bool:
    """CUSIP check digit (the 9th character)."""
    if not cusip or len(cusip) != 9:
        return False
    total = 0
    for i, ch in enumerate(cusip[:8]):
        v = int(ch) if ch.isdigit() else (ord(ch) - 55 if ch.isalpha() else {"*": 36, "@": 37, "#": 38}.get(ch, -1))
        if v < 0:
            return False
        v *= 2 if i % 2 else 1
        total += v // 10 + v % 10
    return str((10 - total % 10) % 10) == cusip[8]


def ftd_frame(raw_path: Path) -> pl.DataFrame:
    """One row per (symbol, CUSIP, description) in a half-month file."""
    with zipfile.ZipFile(raw_path) as z:
        text = z.read(z.namelist()[0]).decode("latin-1")
    rows = []
    for line in text.splitlines()[1:]:
        parts = line.split("|")
        if len(parts) < 6 or not parts[0].strip().isdigit():
            continue  # trailer ("Trailer total quantity of shares ...") and blank lines
        rows.append((parts[0].strip(), parts[1].strip() or None, parts[2].strip() or None, parts[4].strip() or None,
                     parts[5].strip()))
    period = re.search(r"cnsfails(\d{6}[ab])", raw_path.name).group(1)
    df = pl.DataFrame(rows, schema=["d", "cusip", "symbol", "description", "price"], orient="row")
    df = df.with_columns(pl.col("d").str.to_date("%Y%m%d"), pl.col("price").cast(pl.Float64, strict=False))
    out = (df.sort("d").group_by("symbol", "cusip", "description", maintain_order=True)
           .agg(first_date=pl.col("d").min(), last_date=pl.col("d").max(), days=pl.col("d").n_unique(),
                last_price=pl.col("price").last()))
    return out.with_columns(ingested_at=pl.lit(_ingested_at(raw_path)), period=pl.lit(period)).select(
        [pl.col(c).cast(t) for c, t in FTD_SCHEMA.items()])


def stage_ftd(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(ftd_frame(raw_path),
                                errors={"required_fields": pl.col("symbol").is_null() | pl.col("cusip").is_null()},
                                warns={"cusip_check_digit": ~pl.col("cusip").map_elements(cusip_ok, return_dtype=pl.Boolean)})
    return write_staged(raw_path, staged, quarantined), staged, quarantined


def fund_frame(raw_path: Path) -> pl.DataFrame:
    content = raw_path.read_bytes()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    df = pl.read_csv(io.StringIO(text), infer_schema_length=0, null_values=["[NULL]", ""], truncate_ragged_lines=True)
    df = df.rename({c: FUND_COLUMNS[c.strip()] for c in df.columns if c.strip() in FUND_COLUMNS})
    df = df.with_columns(pl.lit(None, pl.String).alias(c) for c in set(FUND_COLUMNS.values()) - set(df.columns))
    year = int(raw_path.name.split("__")[1])
    return df.with_columns(ingested_at=pl.lit(_ingested_at(raw_path)), year=pl.lit(year),
                           ticker=pl.col("ticker").str.strip_chars().str.to_uppercase().replace("", None)).select(
        [pl.col(c).cast(t) for c, t in FUND_SCHEMA.items()])


def stage_fund_classes(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(fund_frame(raw_path), errors={"required_fields": pl.col("class_id").is_null()},
                                warns={"no_ticker": pl.col("ticker").is_null()})
    return write_staged(raw_path, staged, quarantined), staged, quarantined


def tickers_frame(raw_path: Path) -> pl.DataFrame:
    j = json.loads(raw_path.read_bytes())
    rows = [dict(zip(j["fields"], r, strict=True)) for r in j["data"]]
    return pl.DataFrame([{"ingested_at": _ingested_at(raw_path), "cik": f"{int(r['cik']):010d}", "name": r.get("name"),
                          "ticker": r.get("ticker"), "exchange": r.get("exchange")} for r in rows], schema=TICKER_SCHEMA)


def stage_tickers(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(tickers_frame(raw_path), errors={"required_fields": pl.col("ticker").is_null()},
                          warns={"no_exchange": pl.col("exchange").is_null()})
    return write_staged(raw_path, staged, quarantined), staged, quarantined


def backfill(data_dir: Path) -> dict:
    """First fetch (by hand, after the US close): fund files, fail-to-deliver from 2016, today's tickers."""
    return {"sec_fund_classes": fetch_fund_classes(data_dir), "sec_ftd": fetch_ftd(data_dir),
            "sec_tickers": fetch_tickers(data_dir)}


if __name__ == "__main__":
    data = Path(os.environ.get("DATA_DIR", "data"))
    cmd = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    fn = {"backfill": backfill, "ftd": fetch_ftd, "funds": fetch_fund_classes, "tickers": fetch_tickers}[cmd]
    print(json.dumps(fn(data), indent=2, default=str))
