from datetime import date, datetime, timedelta, timezone

import polars as pl

from pit.slo import compute, sessions
from pit.storage import TS

OPEN = datetime(2026, 10, 23, 13, 30, tzinfo=timezone.utc)  # 09:30 ET, 78 slots to 16:00
MIN = timedelta(minutes=1)


def run(slot_min, status, finished_min):
    slot = OPEN + slot_min * MIN
    return {"run_id": f"scheduled__{slot.isoformat()}", "status": status, "started_at": slot, "finished_at": slot + finished_min * MIN}


def test_slis_count_slots_by_run_id_and_bars_against_the_calendar(tmp_path):
    runs = pl.DataFrame([
        run(0, "success", 3),                                  # 09:30 on time
        run(5, "failed", 6), run(5, "success", 12),            # 09:35 failed, its retry lands 12 min later: covered, late
        run(10, "failed", 1),                                  # 09:40 lost
        {"run_id": "cli__20261023T140000Z", "status": "success", "started_at": OPEN, "finished_at": OPEN},  # not a slot
    ], schema={"run_id": pl.String, "status": pl.String, "started_at": TS, "finished_at": TS})
    d = tmp_path / "pipeline_runs/dataset=option_snapshots/ingest_date=2026-10-23"
    d.mkdir(parents=True)
    runs.write_parquet(d / "runs.parquet")
    bars = pl.DataFrame({"symbol": ["SPY", "SPY", "QQQ", "SPY"],                       # SPY 09:30 twice (two versions)
                         "bar_start": [OPEN, OPEN, OPEN, OPEN - timedelta(minutes=5)]},  # pre-market bar: not counted
                        schema={"symbol": pl.String, "bar_start": TS})
    b = tmp_path / "staged/equity_bars/ingest_date=2026-10-23"
    b.mkdir(parents=True)
    bars.write_parquet(b / "bars.parquet")

    rows = compute(tmp_path, sessions(date(2026, 10, 23), 1), n_symbols=2).to_dicts()
    day, window = rows
    assert day["session"] == "2026-10-23" and window["session"] == "window"
    assert (day["option_slots"], day["option_ok"]) == (78, 2)
    assert day["sli2_on_time_pct"] == round(100 / 78, 2)
    assert (day["bars_present"], day["bars_expected"]) == (2, 156)


def test_half_day_has_fewer_slots():
    (_, open_, close), = sessions(date(2026, 11, 27), 1)   # day after Thanksgiving: closes 13:00 ET
    assert (close - open_) / timedelta(minutes=5) == 42
