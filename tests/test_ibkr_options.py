import json
import math
from datetime import date, datetime, timezone

import pytest

from ib_async import Option, Ticker

from pit.ibkr_options import due, keep_strike, pick_expiries, ticker_row
from pit.storage import write_raw


EXPIRIES = ["20261013", "20261014", "20261015", "20261016", "20261021", "20261113", "20261218", "20270115"]
TODAY = date(2026, 10, 14)


def test_pick_expiries_by_dte_range():
    assert pick_expiries(EXPIRIES, TODAY, {"dte": [0, 2]}) == ["20261014", "20261015", "20261016"]


def test_pick_expiries_nearest_to_targets_deduplicated():
    # 7 -> 21 Oct (7d), 30 -> 13 Nov (30d), 45 -> 13 Nov again (dedup), 90 -> 15 Jan (93d) beats 18 Dec (65d)
    assert pick_expiries(EXPIRIES, TODAY, {"target_dte": [7, 30, 45, 90]}) == ["20261021", "20261113", "20270115"]


TERM = {"rights": "otm", "put_range": 0.40, "call_range": 0.15, "strike_steps": [[0.10, 25], [1.0, 50]]}


@pytest.mark.parametrize("strike, right, keep", [
    (7800, "P", True), (7800, "C", True),          # ATM: both sides
    (7900, "P", False), (7700, "C", False),        # ITM: dropped (parity)
    (7725, "P", True), (7730, "P", False),         # within 10%: multiples of 25 only
    (6500, "P", True), (6525, "P", False),         # beyond 10%: multiples of 50 only
    (4700, "P", True), (4650, "P", False),         # -40% edge
    (8950, "C", True), (9000, "C", False),         # +15% edge
])
def test_keep_strike_otm_wings(strike, right, keep):
    assert keep_strike(strike, right, 7800.0, TERM) is keep


def test_keep_strike_both_sides_every_strike():
    chain = {"rights": "both", "put_range": 0.05, "call_range": 0.05, "strike_steps": [[1.0, 5]]}
    assert keep_strike(7900, "P", 7800.0, chain) and keep_strike(7805, "C", 7800.0, chain)
    assert not keep_strike(8200, "C", 7800.0, chain)


def test_due_by_every_minutes():
    at = lambda h, m: datetime(2026, 10, 14, h, m)
    assert due({}, at(9, 37)) and due({"every_minutes": 15}, at(9, 47)) and not due({"every_minutes": 15}, at(9, 50))


def test_ticker_row_ask_only_quote():
    # ib_async turns an empty side (size 0) into NaN; deep OTM typically looks like this.
    t = Ticker(contract=Option("SPX", "20261013", 7500, "C", tradingClass="SPXW"))
    t.bid, t.ask, t.askSize = math.nan, 0.05, 10.0
    row = ticker_row(t)
    assert row["bid"] is None and row["ask"] == 0.05 and row["last"] is None
    assert row["modelGreeks"] is None
    json.dumps(row)  # must be JSON-serializable


def test_write_raw_is_partitioned_and_complete(tmp_path):
    at = datetime(2026, 10, 13, 14, 30, tzinfo=timezone.utc)
    path = write_raw(tmp_path, "ibkr", "option_snapshots", {"x": 1}, at)
    assert path.parent == tmp_path / "raw/source=ibkr/ingest_date=2026-10-13/option_snapshots"
    assert json.loads(path.read_text()) == {"x": 1}
    assert not list(path.parent.glob("*.tmp"))


def test_index_snapshot_staging(tmp_path):
    from pit.ibkr_indices import stage_file
    at = datetime(2026, 10, 15, 14, 0, tzinfo=timezone.utc)
    rows = [{"symbol": "VIX", "exchange": "CBOE", "price": 17.2, "last": 17.2, "close": 16.9, "bid": -1, "ask": -1,
             "marketDataType": 1, "tick_time": at.isoformat()},
            {"symbol": "SKEW", "exchange": "CBOE", "price": None, "last": None, "close": 142.0, "bid": None, "ask": None,
             "marketDataType": 1, "tick_time": None}]
    raw = write_raw(tmp_path, "ibkr", "index_snapshots", {"ingested_at": at.isoformat(), "indices": rows}, at)
    _, staged, quarantined = stage_file(raw)
    assert quarantined.height == 0 and staged["bid"].to_list() == [None, None]
    assert staged["flags"].to_list() == [[], ["no_live_price"]]



def test_listed_contracts_cached_per_day(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from pit.ibkr_options import listed_contracts
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    calls = []

    class FakeIB:
        def reqContractDetails(self, template):
            calls.append(template.lastTradeDateOrContractMonth)
            return [SimpleNamespace(contract=Option("SPX", "20261119", k, r, "SMART", "100", "USD", conId=k * 10 + (r == "P"),
                                                    localSymbol=f"SPX {k}{r}", tradingClass="SPX"))
                    for k in (7000.0, 7025.0) for r in ("C", "P")]

    chain = {"symbol": "SPX", "trading_class": "SPX", "currency": "USD"}
    first = listed_contracts(FakeIB(), chain, "20261119", TODAY)
    second = listed_contracts(FakeIB(), chain, "20261119", TODAY)       # served from the day's cache
    assert calls == ["20261119"] and len(first) == len(second) == 4
    assert [(c.conId, c.strike, c.right, c.localSymbol) for c in second] == [(c.conId, c.strike, c.right, c.localSymbol) for c in first]
    listed_contracts(FakeIB(), chain, "20261119", date(2026, 10, 15))  # a new day asks again
    assert calls == ["20261119", "20261119"]


def test_carry_forward_copies_latest_earlier_day_only_when_missing(tmp_path, monkeypatch):
    from pit.ibkr_options import carry_forward
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    root = tmp_path / "cache/ibkr_contracts"
    for day, body in (("2026-10-14", "old"), ("2026-10-16", "friday")):
        (root / day).mkdir(parents=True)
        (root / day / "SPX_SPX_20261119.json").write_text(body)
    (root / "2026-10-19").mkdir(parents=True)
    (root / "2026-10-19" / "SPX_SPX_20261217.json").write_text("fresh")
    chain = {"symbol": "SPX", "trading_class": "SPX"}
    n = carry_forward([(chain, "20261119"), (chain, "20261217"), (chain, "20270115")], date(2026, 10, 19))
    assert n == 1
    assert (root / "2026-10-19/SPX_SPX_20261119.json").read_text() == "friday"   # latest earlier day
    assert (root / "2026-10-19/SPX_SPX_20261217.json").read_text() == "fresh"    # today's kept
    assert not (root / "2026-10-19/SPX_SPX_20270115.json").exists()            # nothing to carry


@pytest.mark.parametrize("expiry, monthly", [
    ("20261016", True), ("20261009", False), ("20261023", False), ("20261015", False),
    ("20250417", True), ("20250418", False),   # Good Friday 18 Apr 2025 (third Friday): monthly moved to Thursday
])
def test_is_monthly(expiry, monthly):
    from pit.ibkr_options import is_monthly
    assert is_monthly(expiry) is monthly


def test_due_with_offset():
    at = lambda h, m: datetime(2026, 10, 19, h, m)
    chain = {"every_minutes": 15, "offset_minutes": 5}
    assert due(chain, at(9, 35)) and due(chain, at(9, 50)) and not due(chain, at(9, 30)) and not due(chain, at(9, 45))


def test_monthly_filter_ignores_leaps_beyond_calendar():
    exps = ["20261023", "20261120", "20261218", "20270115", "20271217", "20290119"]
    assert pick_expiries(exps, TODAY, {"dte": [3, 120], "monthly": True}) == ["20261120", "20261218", "20270115"]
    from pit.ibkr_options import is_monthly
    assert is_monthly("20290119")  # far beyond the calendar: plain third Friday


def test_one_failing_chain_does_not_sink_the_run(tmp_path, monkeypatch):
    import pit.ibkr_options as io
    cfg = tmp_path / "ibkr.toml"
    cfg.write_text('[ibkr]\nhost="x"\nport=1\nclient_id=1\nmarket_data_type=1\n'
                   '[[chains]]\nname="good"\n[[chains]]\nname="bad"\n')
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(io, "connect", lambda cfg, client_offset=0: (type("IB", (), {"disconnect": lambda self: None})(), []))
    monkeypatch.setattr(io, "due", lambda chain, now: True)

    def snap(ib, ib_cfg, chain, today, spots, taken, deadline=None):
        if chain["name"] == "bad":
            raise TimeoutError()
        return {"chain": "good", "contracts": []}

    monkeypatch.setattr(io, "snapshot_chain", snap)
    payload = json.loads(io.run(str(cfg)).read_text())
    assert [u["chain"] for u in payload["underlyings"]] == ["good"]
    assert payload["chain_errors"][0]["chain"] == "bad" and "TimeoutError" in payload["chain_errors"][0]["error"]


def test_run_budget_skips_remaining_chains_but_keeps_the_done_ones(tmp_path, monkeypatch):
    import time as _time
    import pit.ibkr_options as io
    cfg = tmp_path / "ibkr.toml"
    cfg.write_text('[ibkr]\nhost="x"\nport=1\nclient_id=1\nmarket_data_type=1\nrun_budget_seconds=0.05\n'
                   '[[chains]]\nname="spx"\n[[chains]]\nname="late"\n')
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(io, "connect", lambda cfg, client_offset=0: (type("IB", (), {"disconnect": lambda self: None})(), []))
    monkeypatch.setattr(io, "due", lambda chain, now: True)

    def snap(ib, ib_cfg, chain, today, spots, taken, deadline=None):
        _time.sleep(0.1)  # the first chain uses up the budget
        return {"chain": chain["name"], "contracts": []}

    monkeypatch.setattr(io, "snapshot_chain", snap)
    payload = json.loads(io.run(str(cfg)).read_text())
    assert [u["chain"] for u in payload["underlyings"]] == ["spx"]
    assert payload["chain_errors"] == [{"chain": "late", "error": "skipped: run budget used up"}]


def test_warm_cache_makes_runs_free_of_secdef_requests(tmp_path, monkeypatch):
    # After the day's first answer, underlying + expirations come from the cache: a stalled secdef server can't hang a run.
    from types import SimpleNamespace
    from pit.ibkr_options import underlying
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    calls = []

    class FakeIB:
        def qualifyContracts(self, c):
            calls.append("qualify")
            c.conId = 416904
            return [c]

        def reqSecDefOptParams(self, *a):
            calls.append("secdef")
            return [SimpleNamespace(tradingClass="SPXW", exchange="SMART", expirations={"20261120", "20261113"}),
                    SimpleNamespace(tradingClass="SPX", exchange="SMART", expirations={"20261119"})]

    class Stalled:
        def __getattr__(self, name):
            raise AssertionError(f"{name} called: the run should be served from the cache")

    chain = {"symbol": "SPX", "sec_type": "IND", "exchange": "CBOE", "currency": "USD", "trading_class": "SPXW"}
    und, exps = underlying(FakeIB(), chain, TODAY, refresh=True)                # warm_cache
    assert calls == ["qualify", "secdef"] and exps == ["20261113", "20261120"]
    und2, exps2 = underlying(Stalled(), chain, TODAY)                           # a run later that day
    assert (und2.conId, und2.secType, und2.exchange, exps2) == (416904, "IND", "CBOE", exps)
