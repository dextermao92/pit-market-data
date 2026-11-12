"""The eval cases and the fault-injected data copies they run on."""

import hashlib
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from pit.copilot import fixtures
from pit.copilot.cases import load_cases, problems
from pit.copilot.fixtures import apply_faults, build_base, build_case
from test_mcp_server import write_runs


def digest(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_case_files_are_valid():
    cases = load_cases()
    ids = [c["id"] for c in cases]
    assert len(ids) == len(set(ids)) >= 25
    assert {c["id"]: problems(c) for c in cases if problems(c)} == {}
    assert all(c["simulated"] == (c["_kind"] == "simulated") for c in cases)


@pytest.fixture
def prod(tmp_path):
    """A tiny production data/: write_runs' 2026-10-26 runs plus a day of equity bars."""
    data = tmp_path / "data"
    write_runs(data)
    t0 = datetime(2026, 10, 26, 13, 30, tzinfo=timezone.utc)
    bars = pl.DataFrame({"ingested_at": [t0] * 6, "symbol": ["SPY"] * 6,
                         "bar_start": [t0 + timedelta(minutes=5 * i) for i in range(6)], "close": [1.0] * 6})
    (data / "staged/equity_bars/ingest_date=2026-10-26").mkdir(parents=True)
    bars.write_parquet(data / "staged/equity_bars/ingest_date=2026-10-26/b.parquet")
    return data


def test_base_is_a_copy_and_production_is_untouched(prod, tmp_path):
    before = digest(prod)
    base = tmp_path / "fx" / "base"
    m = build_base(prod, base)
    assert m["files"] == {"pipeline_runs": 2, "staged/equity_bars": 1}
    with pytest.raises(FileExistsError):
        build_base(prod, base)
    with pytest.raises(ValueError):
        build_base(prod, prod / "inside")
    assert digest(prod) == before


def test_simulated_case_leaves_base_and_production_alone(prod, tmp_path):
    base = tmp_path / "fx" / "base"
    build_base(prod, base)
    before_base, before_prod = digest(base), digest(prod)
    sim = {"clone": {"from": "2026-10-26", "to": "2026-10-27"},
           "faults": [{"action": "fail", "datasets": ["option_snapshots"], "from": "09:15", "to": "09:30",
                       "error": "OSError(28, 'No space left on device')", "duration_s": 2},
                      {"action": "drop_bars", "from": "09:40", "to": "09:50"}]}
    root = build_case("s-test", sim, base, tmp_path / "fx")
    assert digest(base) == before_base and digest(prod) == before_prod

    runs = fixtures._read_runs(root)
    day = runs.filter(pl.col("started_at").dt.date() == date(2026, 10, 27))
    assert day.height == 4 and day["started_at"].min() == datetime(2026, 10, 27, 13, 10, tzinfo=timezone.utc)
    failed = day.filter(pl.col("error").str.starts_with("OSError")).sort("started_at")
    assert failed["started_at"].dt.convert_time_zone("America/New_York").dt.strftime("%H:%M").to_list() == \
        ["09:15", "09:20"]
    bars = pl.read_parquet(root / "staged/equity_bars/ingest_date=2026-10-27/sim.parquet")
    assert bars.height == 4  # 09:30..09:55 minus 09:40 and 09:45
    with pytest.raises(ValueError):  # never clone over a day that has data
        build_case("s-bad", sim | {"clone": {"from": "2026-10-26", "to": "2026-10-26"}}, base, tmp_path / "fx")


def test_fault_actions_and_gaps():
    t = lambda h, m: datetime(2026, 11, 12, h, m, tzinfo=timezone.utc)  # 13:00 UTC = 08:00 ET
    day = pl.DataFrame({"run_id": ["a", "b", "c", "d"], "dataset": ["option_snapshots"] * 4,
                        "started_at": [t(14, 0), t(14, 5), t(14, 10), t(14, 15)],
                        "finished_at": [t(14, 1), t(14, 6), t(14, 11), t(14, 16)], "status": ["success"] * 4,
                        "error": [None] * 4, "rows": [10] * 4,
                        "ibkr_error_codes": [[]] * 4}, schema_overrides={"ibkr_error_codes": pl.List(pl.Int64)})
    on = date(2026, 11, 12)
    out = apply_faults(day, [{"action": "fail", "datasets": "all", "from": "09:05", "to": "09:06", "duration_s": 300,
                              "ibkr_error_codes": [1100]},
                             {"action": "shift", "datasets": ["option_snapshots"], "from": "09:10", "to": "09:11",
                              "seconds": 120},
                             {"action": "set", "datasets": "all", "from": "09:15", "to": "09:16",
                              "fields": {"rows": 0}},
                             {"action": "drop", "datasets": "all", "from": "09:00", "to": "09:01"}], on)
    assert out["run_id"].to_list() == ["b", "c", "d"]
    b, c, d = out.iter_rows(named=True)
    assert (b["status"], b["rows"], b["ibkr_error_codes"], b["finished_at"]) == ("failed", None, [1100], t(14, 10))
    assert c["started_at"] == t(14, 12) and d["rows"] == 0
    prior = day.filter(pl.col("run_id") == "a")
    assert fixtures.recompute_gaps(out, prior)["gap_s"].to_list() == [300.0, 720.0, 180.0]
    with pytest.raises(ValueError):
        apply_faults(day, [{"action": "explode", "from": "09:00", "to": "10:00"}], on)
