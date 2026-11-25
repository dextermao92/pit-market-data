"""Stage 2 P1a: whole-market fetches, symbol discovery and the resumable daily-bar backfill."""

import json
from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl
import pytest

from pit import alpaca_bars, assets, corporate_actions, universe
from pit.storage import write_raw

UTC = timezone.utc
CONFIG = """
[alpaca]
feed = "sip"
bar_minutes = 5
lag_minutes = 15
symbols = ["SPY", "BRK.B"]

[universe]
requests_per_minute = 6000
max_symbol_letters = 2
probe_batch = 3
backfill_bars_per_batch = 100
recent_batch = 2
"""


@pytest.fixture(autouse=True)
def outside_collection_window(monkeypatch):
    """Tests run at any hour: put the next collection window far away unless a test sets its own."""
    far = datetime(2100, 1, 4, 13, tzinfo=UTC)
    monkeypatch.setattr(universe, "collection_window", lambda now: (far, far + timedelta(hours=8)))


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("APCA_API_KEY_ID", "k")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "s")
    path = tmp_path / "alpaca.toml"
    path.write_text(CONFIG, encoding="utf-8")
    return str(path)


def mock_client(handler):
    return lambda: httpx.Client(base_url="https://data.alpaca.markets", transport=httpx.MockTransport(handler))


def write_assets(tmp_path, at=datetime(2026, 11, 25, 22, tzinfo=UTC)):
    listed = [{"id": "1", "symbol": "SPY", "exchange": "ARCA", "status": "active", "class": "us_equity", "tradable": True},
              {"id": "2", "symbol": "XLNX", "exchange": "NASDAQ", "status": "inactive", "class": "us_equity", "tradable": False},
              {"id": "3", "symbol": "SGPPY", "exchange": "OTC", "status": "active", "class": "us_equity", "tradable": True}]
    write_raw(tmp_path, "alpaca", "assets", {"ingested_at": at.isoformat(), "duration_s": 1, "assets": listed}, at)


def write_actions(tmp_path, scope, actions, at):
    payload = {"ingested_at": at.isoformat(), "duration_s": 1, "scope": scope, "request": {},
               "pages": [{"corporate_actions": actions, "next_page_token": None}]}
    write_raw(tmp_path, "alpaca", "corporate_actions", payload, at)


def test_brute_force_covers_every_symbol_up_to_n_letters():
    assert len(universe.brute_force_symbols(1)) == 26
    assert len(universe.brute_force_symbols(3)) == 26 + 26**2 + 26**3
    assert universe.brute_force_symbols(2)[-1] == "ZZ"


def test_candidate_sources_merge_lists_skip_otc_and_invalid(cfg, tmp_path):
    write_assets(tmp_path)
    old = datetime(2026, 11, 20, tzinfo=UTC)
    write_actions(tmp_path, "window", {"cash_mergers": [{"acquiree_symbol": "STALE"}]}, old)  # before the full fetch
    write_actions(tmp_path, "full", {"name_changes": [{"old_symbol": "FB", "new_symbol": "META"}],
                                     "cash_mergers": [{"acquiree_symbol": "TWTR", "acquirer_symbol": None}]},
                  datetime(2026, 11, 21, tzinfo=UTC))
    write_actions(tmp_path, "window", {"spin_offs": [{"source_symbol": "GE", "new_symbol": "bad sym"}]},
                  datetime(2026, 11, 24, tzinfo=UTC))
    sources = universe.candidate_sources(cfg, tmp_path)
    assert sources["A"] == ["brute_force"] and sources["GE"] == ["brute_force", "corporate_actions"]
    assert sources["SPY"] == ["assets", "config"] and sources["XLNX"] == ["assets"]   # inactive kept
    assert {"FB", "META", "TWTR", "BRK.B"} <= set(sources)
    assert "SGPPY" not in sources and "bad sym" not in sources and "STALE" not in sources


def test_probe_isolates_rejected_symbols(monkeypatch):
    seen = []

    def handler(request):
        syms = request.url.params["symbols"].split(",")
        seen.append(syms)
        if "BAD" in syms:
            return httpx.Response(400, json={"message": "invalid symbol"})
        return httpx.Response(200, json={"bars": {s: [{"t": "2016-01-01T05:00:00Z"}] for s in syms if s != "NONE"},
                                         "next_page_token": None})

    with mock_client(handler)() as c:
        requests, rejected = universe.probe(c, ["A", "BAD", "NONE", "Z"], "2016-01-01", "2026-11-25", 0)
    assert rejected == ["BAD"]
    assert sorted(s for r in requests for s in r["symbols"]) == ["A", "NONE", "Z"]  # tried-but-empty is recorded
    assert seen[0] == ["A", "BAD", "NONE", "Z"]


def test_discover_writes_raw_per_batch_and_candidates(cfg, tmp_path, monkeypatch):
    def handler(request):
        syms = request.url.params["symbols"].split(",")
        assert request.url.params["timeframe"] == "1Month"
        bars = {s: [{"t": f"2016-0{m}-01T05:00:00Z"} for m in (1, 2, 3)] for s in syms if s in ("A", "SPY", "BRK.B")}
        return httpx.Response(200, json={"bars": bars, "next_page_token": None})

    monkeypatch.setattr(universe, "client", mock_client(handler))
    path = universe.discover(cfg, now=datetime(2026, 11, 25, 22, tzinfo=UTC))
    found = pl.read_parquet(path)
    assert found["symbol"].to_list() == ["A", "BRK.B", "SPY"]
    assert found["n_months"].to_list() == [3, 3, 3] and found["first_month"][0] == date(2016, 1, 1)
    assert found.filter(pl.col("symbol") == "SPY")["sources"][0].to_list() == ["config"]  # no assets file here
    probes = list(tmp_path.glob("raw/source=alpaca/ingest_date=*/universe_probe/*.json"))
    tried = [s for f in probes for r in json.loads(f.read_text())["requests"] for s in r["symbols"]]
    assert len(probes) == -(-len(tried) // 3) and len(tried) == 26 + 26**2 + 2  # brute force + config symbols


def test_discover_rechecks_listed_symbols_that_returned_nothing(cfg, tmp_path, monkeypatch):
    write_actions(tmp_path, "full", {"name_changes": [{"old_symbol": "FB", "new_symbol": "META"}]},
                  datetime(2026, 11, 21, tzinfo=UTC))
    seen = []

    def handler(request):
        syms = request.url.params["symbols"].split(",")
        seen.append(syms)
        keys = [s for s in syms if s in ("FB", "META") and not (s == "FB" and "META" in syms)]  # META wins
        return httpx.Response(200, json={"bars": {k: [{"t": "2016-01-01T05:00:00Z"}] for k in keys},
                                         "next_page_token": None})

    monkeypatch.setattr(universe, "client", mock_client(handler))
    monkeypatch.setattr(universe, "brute_force_symbols", lambda n: ["NOPE"])
    found = pl.read_parquet(universe.discover(cfg, now=datetime(2026, 11, 25, 22, tzinfo=UTC)))
    assert found["symbol"].to_list() == ["FB", "META"]
    assert seen[0] == ["BRK.B", "FB", "META"]
    assert any("FB" in s and "META" not in s for s in seen[1:])  # rechecked without its partner
    assert all("NOPE" not in s for s in seen[2:])  # brute-force-only misses are not rechecked


def test_plan_batches_caps_bars_and_symbols():
    cands = pl.DataFrame({"symbol": ["A", "B", "C", "D", "E"], "n_months": [4, 1, 1, 1, 10],
                          "first_month": [date(2016, 1, 1)] * 4 + [date(2010, 1, 1)],
                          "last_month": [date(2016, 4, 1)] + [date(2016, 1, 1)] * 3 + [date(2010, 10, 1)]},
                         schema_overrides={"n_months": pl.UInt32})
    # A: 84 est. bars, B-D: 21 each; E ended before the range and is left out
    assert universe.plan_batches(cands, date(2016, 1, 1), date(2026, 11, 24), 100, 10) == [["A"], ["B", "C", "D"]]
    assert universe.plan_batches(cands, date(2016, 1, 1), date(2026, 11, 24), 1000, 2) == [["A", "B"], ["C", "D"]]


def test_backfill_is_resumable(cfg, tmp_path, monkeypatch):
    pl.DataFrame({"symbol": ["A", "B", "C"], "n_months": [4, 4, 4], "first_month": [date(2016, 1, 1)] * 3,
                  "last_month": [date(2016, 4, 1)] * 3, "sources": [["brute_force"]] * 3},
                 schema_overrides={"n_months": pl.UInt32}).write_parquet(
        (tmp_path / "cache" / "universe").mkdir(parents=True) or tmp_path / universe.CANDIDATES)
    calls, fail = [], {"B"}
    monkeypatch.setattr(universe.pipeline, "collect_daily_bars", fake_collect(tmp_path, calls, fail))
    universe.backfill(cfg, date(2016, 1, 1), date(2016, 4, 29))
    assert calls == [["A"], ["B"], ["C"]]
    fail.clear()
    universe.backfill(cfg, date(2016, 1, 1), date(2016, 4, 29))
    assert calls[3:] == [["B"]]  # only the failed batch is fetched again


def fake_collect(tmp_path, calls, fail=(), same_security=(), starts=None):
    """collect_daily_bars stand-in: writes a raw file whose bars mimic Alpaca, returning a security once per
    request when several requested symbols map to it (the later symbol wins)."""
    def collect(config_path, sessions, run_id, symbols):
        calls.append(symbols)
        if starts is not None:
            starts.append(min(sessions))
        if set(symbols) & set(fail):
            raise RuntimeError("vendor down")
        keys = [s for s in symbols if not any(a == s and b in symbols for a, b in same_security)]
        at = datetime(2026, 11, 25, 22, tzinfo=UTC) + timedelta(seconds=len(calls))
        raw = write_raw(tmp_path, "alpaca", "daily_bars", {"pages": [{"bars": {k: [{"t": "x"}] for k in keys}}]}, at)
        return {"run_id": run_id, "rows": len(keys), "quarantined": 0, "cycle_s": 0.1, "raw_path": str(raw)}
    return collect


def test_backfill_rechecks_symbols_swallowed_by_their_partner(cfg, tmp_path, monkeypatch):
    pl.DataFrame({"symbol": ["SIVB", "SIVBQ", "ZZ"], "n_months": [1, 1, 1], "first_month": [date(2016, 1, 1)] * 3,
                  "last_month": [date(2016, 1, 1)] * 3, "sources": [["brute_force"]] * 3},
                 schema_overrides={"n_months": pl.UInt32}).write_parquet(
        (tmp_path / "cache" / "universe").mkdir(parents=True) or tmp_path / universe.CANDIDATES)
    calls = []
    monkeypatch.setattr(universe.pipeline, "collect_daily_bars",
                        fake_collect(tmp_path, calls, same_security=[("SIVB", "SIVBQ")]))
    universe.backfill(cfg, date(2016, 1, 1), date(2016, 1, 29))
    assert calls == [["SIVB", "SIVBQ", "ZZ"], ["SIVB"]]  # SIVB came back empty in the main batch, alone after


def test_recent_symbols_are_active_listed_plus_config(cfg, tmp_path, monkeypatch):
    write_assets(tmp_path)
    assert universe.recent_symbols(cfg) == ["BRK.B", "SPY"]  # XLNX inactive, SGPPY OTC
    batches = []
    monkeypatch.setattr(universe.pipeline, "collect_daily_bars",
                        lambda c, s, run_id, symbols: batches.append((run_id, symbols)) or {})
    universe.collect_recent_daily_bars(cfg, [date(2026, 11, 24)], "eod1")
    assert batches == [("eod1__b000", ["BRK.B", "SPY"])]


def test_fetch_pages_waits_out_rate_limit(monkeypatch):
    slept, calls = [], []
    monkeypatch.setattr(alpaca_bars.time, "sleep", slept.append)

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"x-ratelimit-reset": "0"})
        return httpx.Response(200, json={"bars": {}, "next_page_token": None})

    with mock_client(handler)() as c:
        assert alpaca_bars.fetch_pages(c, "/v2/stocks/bars", {}) == [{"bars": {}, "next_page_token": None}]
    assert len(calls) == 2 and slept == [1.0]  # reset already passed: the 1 s floor


def test_fetch_pages_gives_up_after_repeated_rate_limits(monkeypatch):
    monkeypatch.setattr(alpaca_bars.time, "sleep", lambda s: None)
    with mock_client(lambda r: httpx.Response(429))() as c, pytest.raises(httpx.HTTPStatusError):
        alpaca_bars.fetch_pages(c, "/v2/stocks/bars", {}, max_rate_limited=2)


def test_assets_fetches_whole_market_and_records_missing(cfg, tmp_path, monkeypatch):
    def handler(request):
        status = request.url.params["status"]
        return httpx.Response(200, json=[{"id": status, "symbol": "SPY" if status == "active" else "XLNX",
                                          "exchange": "ARCA", "class": "us_equity", "status": status,
                                          "tradable": status == "active"}])

    real = httpx.Client
    monkeypatch.setattr(assets.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    payload = json.loads(assets.run(cfg, now=datetime(2026, 11, 25, 22, tzinfo=UTC)).read_text())
    assert [a["symbol"] for a in payload["assets"]] == ["SPY", "XLNX"]
    assert payload["missing"] == ["BRK.B"] and payload["scope"] == "all"


@pytest.mark.parametrize("full, start", [(False, "2026-10-26"), (True, "2016-01-01")])
def test_corporate_actions_window_or_full_without_symbol_filter(cfg, monkeypatch, full, start):
    seen = []

    def handler(request):
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"corporate_actions": {}, "next_page_token": None})

    monkeypatch.setattr(corporate_actions, "client", mock_client(handler))
    payload = json.loads(corporate_actions.run(cfg, now=datetime(2026, 11, 25, 22, tzinfo=UTC), full=full).read_text())
    assert seen == [{"start": start, "end": "2027-11-25", "limit": "1000"}]
    assert payload["scope"] == ("full" if full else "window")


def write_candidates(tmp_path, symbols, first=date(2016, 1, 1), last=date(2016, 6, 1), n=6):
    (tmp_path / "cache" / "universe").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols, "n_months": [n] * len(symbols), "first_month": [first] * len(symbols),
                  "last_month": [last] * len(symbols), "sources": [["brute_force"]] * len(symbols)},
                 schema_overrides={"n_months": pl.UInt32}).write_parquet(tmp_path / universe.CANDIDATES)


def test_backfill_is_incremental(cfg, tmp_path, monkeypatch):
    calls, starts = [], []
    monkeypatch.setattr(universe.pipeline, "collect_daily_bars", fake_collect(tmp_path, calls, starts=starts))
    write_candidates(tmp_path, ["A", "B"])
    first = universe.backfill(cfg, date(2016, 1, 1), date(2016, 4, 29))
    assert calls == [["A"], ["B"]] and starts == [date(2016, 1, 4)] * 2 and first["new_symbols"] == 2
    write_candidates(tmp_path, ["A", "B", "C"])  # C: discovered later
    second = universe.backfill(cfg, date(2016, 1, 1), date(2016, 6, 30))
    assert calls[2:] == [["A"], ["B"], ["C"]]
    assert starts[2:] == [date(2016, 5, 2)] * 2 + [date(2016, 1, 4)]  # A, B topped up from their last end; C in full
    assert second["new_symbols"] == 1 and second["returned"] == 3
    universe.backfill(cfg, date(2016, 1, 1), date(2016, 6, 30))
    assert len(calls) == 5  # nothing left to fetch


def test_plan_batches_estimates_only_the_overlapping_months():
    cands = pl.DataFrame({"symbol": ["A", "B", "C"], "n_months": [120, 120, 120],
                          "first_month": [date(2016, 1, 1)] * 3, "last_month": [date(2025, 12, 1)] * 3},
                         schema_overrides={"n_months": pl.UInt32})
    # a one-month top-up: 21 estimated bars each, so all three fit in one batch of 100
    assert universe.plan_batches(cands, date(2025, 12, 1), date(2025, 12, 31), 100, 10) == [["A", "B", "C"]]
    assert len(universe.plan_batches(cands, date(2016, 1, 1), date(2025, 12, 31), 100, 10)) == 3


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 11, 29, 1, 0, tzinfo=UTC), date(2026, 11, 27)),    # Sat 20:00 ET -> Fri (13:00 half day)
    (datetime(2026, 11, 25, 21, 10, tzinfo=UTC), date(2026, 11, 24)),  # Wed 16:10 ET: today's bar still delayed
    (datetime(2026, 11, 25, 22, 0, tzinfo=UTC), date(2026, 11, 25)),   # Wed 17:00 ET
])
def test_last_closed_session(now, expected):
    assert universe.last_closed_session(now) == expected


def test_refresh_discovers_then_backfills_to_the_last_session(cfg, tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(universe, "discover", lambda c, now=None, stop_by=None: write_candidates(tmp_path, ["A"])
                        or tmp_path / universe.CANDIDATES)
    monkeypatch.setattr(universe, "backfill",
                        lambda c, start, end, stop_by=None: seen.append((start, end)) or {"rows": 1})
    out = universe.refresh(cfg, now=datetime(2026, 11, 29, 1, 0, tzinfo=UTC))
    assert seen == [(date(2016, 1, 1), date(2026, 11, 27))] and out == {"candidates": 1, "rows": 1}


def test_backfill_stops_before_the_collection_window_and_resumes(cfg, tmp_path, monkeypatch):
    t0 = datetime(2026, 11, 25, 23, 0, tzinfo=UTC)
    clock = {"t": t0}
    monkeypatch.setattr(universe, "_now", lambda: clock["t"])
    calls = []
    collect = fake_collect(tmp_path, calls)

    def slow_collect(*a, **kw):  # each batch takes 10 minutes
        clock["t"] += timedelta(minutes=10)
        return collect(*a, **kw)

    monkeypatch.setattr(universe.pipeline, "collect_daily_bars", slow_collect)
    write_candidates(tmp_path, ["A", "B", "C"], last=date(2016, 4, 1), n=4)
    first = universe.backfill(cfg, date(2016, 1, 1), date(2016, 4, 29), stop_by=t0 + timedelta(minutes=12))
    assert calls == [["A"]] and first["stopped"]  # B would start 2 min before the window (5 min margin)
    second = universe.backfill(cfg, date(2016, 1, 1), date(2016, 4, 29), stop_by=t0 + timedelta(hours=9))
    assert calls[1:] == [["B"], ["C"]] and not second["stopped"]


def test_backfill_inside_the_collection_window_fetches_nothing(cfg, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(universe.pipeline, "collect_daily_bars", fake_collect(tmp_path, calls))
    write_candidates(tmp_path, ["A"])
    stats = universe.backfill(cfg, date(2016, 1, 1), date(2016, 6, 30),
                              stop_by=datetime.now(UTC) - timedelta(hours=1))
    assert calls == [] and stats["stopped"]


def test_discover_refuses_without_enough_time(cfg, monkeypatch):
    monkeypatch.setattr(universe, "client", lambda: pytest.fail("must not fetch"))
    with pytest.raises(RuntimeError, match="after the close"):
        universe.discover(cfg, stop_by=datetime.now(UTC) + timedelta(minutes=30))


def test_default_stop_by_is_the_next_window_start(monkeypatch):
    monkeypatch.undo()  # the real calendar
    monkeypatch.setattr(universe, "_now", lambda: datetime(2026, 11, 29, 1, 0, tzinfo=UTC))  # Sat 20:00 ET
    assert universe.default_stop_by() == datetime(2026, 11, 30, 13, 0, tzinfo=UTC)          # Mon 08:00 ET
