from datetime import datetime, timedelta, timezone

import httpx
import polars as pl

from pit import alpaca_bars, pipeline
from pit.alpaca_bars import fetch_pages, to_frame, window
from pit.storage import write_raw

UTC = timezone.utc
BAR = LAG = timedelta(minutes=15)
OPEN = datetime(2026, 10, 14, 13, 30, tzinfo=UTC)   # 09:30 ET
CLOSE = datetime(2026, 10, 14, 20, 0, tzinfo=UTC)   # 16:00 ET


def t(h, m, s=0):
    return datetime(2026, 10, 14, h, m, s, tzinfo=UTC)


def test_window_only_completed_bars_past_the_lag():
    assert window(t(14, 0, 30), OPEN, CLOSE, BAR, LAG) == (OPEN, t(13, 30))   # 10:00 ET: first bar available
    assert window(t(14, 29, 59), OPEN, CLOSE, BAR, LAG) == (OPEN, t(13, 45))
    first, last = window(t(13, 50), OPEN, CLOSE, BAR, LAG)                     # too early: empty
    assert last < first


def test_window_clips_to_session_close_incl_half_days():
    assert window(t(21, 0), OPEN, CLOSE, BAR, LAG)[1] == t(19, 45)
    assert window(t(21, 0), OPEN, t(17, 0), BAR, LAG)[1] == t(16, 45)          # 13:00 ET early close


def bar(ts, c=100.0):
    return {"t": ts, "o": c, "h": c + 1, "l": c - 1, "c": c, "v": 1000, "n": 10, "vw": c}


def test_fetch_pages_follows_page_tokens():
    pages = {None: {"bars": {"SPY": [bar("2026-10-14T13:30:00Z")]}, "next_page_token": "p2"},
             "p2": {"bars": {"QQQ": [bar("2026-10-14T13:30:00Z")]}, "next_page_token": None}}
    seen = []

    def handler(request):
        token = request.url.params.get("page_token")
        seen.append(token)
        return httpx.Response(200, json=pages[token])

    with httpx.Client(base_url="https://x", transport=httpx.MockTransport(handler)) as c:
        out = fetch_pages(c, "/v2/stocks/bars", {"symbols": "SPY,QQQ"})
    assert seen == [None, "p2"] and len(out) == 2


PAYLOAD = {
    "ingested_at": "2026-10-14T14:00:00+00:00", "duration_s": 0.4, "session": "2026-10-14",
    "request": {"symbols": "SPY,BRK.B", "timeframe": "15Min", "start": "2026-10-14T13:30:00+00:00",
                "end": "2026-10-14T13:45:00+00:00"},
    "pages": [{"bars": {"SPY": [bar("2026-10-14T13:30:00Z", 700.0), bar("2026-10-14T13:45:00Z", 701.0)]},
               "next_page_token": "p2"},
              {"bars": {"BRK.B": [bar("2026-10-14T13:30:00Z", 500.0)]}, "next_page_token": None}],
}


def test_to_frame_flattens_all_pages():
    df = to_frame(PAYLOAD)
    assert df.height == 3 and df["symbol"].to_list() == ["SPY", "SPY", "BRK.B"]
    assert df["bar_start"][1] == t(13, 45) and df["volume"].dtype == pl.Int64


def test_pipeline_records_equity_run(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(alpaca_bars, "run", lambda cfg: write_raw(tmp_path, "alpaca", "equity_bars", PAYLOAD, t(14, 0)))
    rec = pipeline.collect_equity_bars("cfg.toml", run_id="e1")
    assert rec["status"] == "success" and rec["rows"] == 3 and rec["quarantined"] == 0
    assert rec["missing_bars"] == 1  # BRK.B has no 13:45 bar
    assert rec["staged_path"].endswith("staged/equity_bars/ingest_date=2026-10-14/20261014T140000000000Z.parquet")
    runs = pl.read_parquet(tmp_path / "pipeline_runs/dataset=equity_bars/**/*.parquet")
    assert runs["two_sided"][0] is None  # option-only columns stay null


def test_window_with_5_min_bars():
    five = timedelta(minutes=5)
    assert window(t(13, 50, 1), OPEN, CLOSE, five, LAG) == (OPEN, t(13, 30))   # 09:50 ET: first 5-min bar
    assert window(t(20, 15, 1), OPEN, CLOSE, five, LAG)[1] == t(19, 55)        # 16:15 ET: last bar of the day
