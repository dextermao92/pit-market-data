"""Confirmed diagnoses -> needs_review eval case drafts, with a data copy for days the frozen base lacks."""

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest
import yaml

from pit.copilot import drafts
from pit.copilot import eval as ev
from pit.copilot.cases import load_cases
from pit.copilot.fixtures import _read_runs, build_base, build_real_case, last_day
from test_mcp_server import RUN_SCHEMA, write_runs

EXISTING = {"id": "r03-2026-10-16-gateway-autorestart", "simulated": False, "today": "2026-10-17",
            "question": "Why did the Gateway restart on 2026-10-16?",
            "expected": {"categories": ["gateway_restarting"], "runbooks": ["runbooks/gateway"]},
            "label": {"status": "verified", "source": "x"}}


def note(_id="20261118T010000-abc123", question="Why were the 2026-11-17 option snapshots missing at 10:00?",
         category="host_down", runbook="runbooks/stale-snapshots", confirmed_on="2026-11-18"):
    return {"_id": _id, "question": question, "category": category, "runbook": runbook,
            "root_cause": "Docker was down after a Windows update.", "evidence": ["option_snapshots TimeoutError"],
            "confirmed_by": "operator", "confirmed_on": confirmed_on}


@pytest.fixture
def cases_dir(tmp_path):
    d = tmp_path / "cases"
    (d / "real").mkdir(parents=True)
    (d / "real" / f"{EXISTING['id']}.yaml").write_text(yaml.safe_dump(EXISTING, sort_keys=False))
    return d


def test_a_note_becomes_a_needs_review_draft(cases_dir):
    out, skipped = drafts.plan([note()], load_cases(cases_dir))
    assert skipped == [] and len(out) == 1
    c = out[0]
    assert c["id"] == "r04-2026-11-17-host-down"  # numbered after the highest real case
    assert (c["today"], c["simulated"]) == ("2026-11-18", False)  # "today": the day it was confirmed
    assert c["expected"] == {"categories": ["host_down"], "runbooks": ["runbooks/stale-snapshots"]}
    assert c["label"]["status"] == "needs_review" and c["label"]["note"] == "20261118T010000-abc123"
    assert "memory note 20261118T010000-abc123" in c["label"]["source"]
    assert "Docker was down" in c["label"]["source"]


def test_duplicates_and_bad_notes_are_skipped_with_a_reason(cases_dir):
    existing = load_cases(cases_dir)
    batch = [note("n1", question="Why did the Gateway restart on 2026-10-16 at 11:00?", category="gateway_restarting"),
             note("n2"), note("n3"),                    # n3: same day and category as n2, in the same batch
             note("n4", question="What broke on 2026-11-17?", category="duckdb_lock"),  # same day, other category
             note("n5", category="made_up"),
             note("n6", question="Why was 2026-11-16 late?", runbook="runbooks/nope")]
    out, skipped = drafts.plan(batch, existing)
    assert [c["label"]["note"] for c in out] == ["n2", "n4"]
    assert [c["id"][:3] for c in out] == ["r04", "r05"]
    text = "\n".join(skipped)
    assert "n1: 2026-10-16 gateway_restarting is already case r03" in text
    assert "n3: 2026-11-17 host_down is already case r04-2026-11-17-host-down" in text
    assert "n5: unknown category" in text and "n6: unknown runbook" in text
    # a note that was drafted before is not drafted again
    assert drafts.plan([note("n2")], existing + [out[0] | {"_kind": "real"}])[1] == ["n2: already drafted"]


def test_day_falls_back_to_confirmed_on_and_account_ids_are_redacted(cases_dir):
    out, _ = drafts.plan([note(question="Why did account U1234567 get no snapshots this morning?")],
                         load_cases(cases_dir))
    assert out[0]["id"] == "r04-2026-11-18-host-down"
    assert "U1234567" not in out[0]["question"] and "confirmed_on" in out[0]["label"]["source"]


def digest(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def add_day(data, day: date, start_utc: datetime, n=3):
    """n successful option runs from start_utc, partitioned by UTC date, plus equity bars, as production writes them."""
    rows = [{"run_id": f"scheduled__{start_utc + i * timedelta(minutes=5)}", "dataset": "option_snapshots",
             "started_at": start_utc + i * timedelta(minutes=5),
             "finished_at": start_utc + i * timedelta(minutes=5, seconds=30), "status": "success", "rows": 100}
            for i in range(n)]
    for r in rows:
        d = data / f"pipeline_runs/dataset=option_snapshots/ingest_date={r['started_at'].date()}"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([r], schema=RUN_SCHEMA).write_parquet(d / f"{r['started_at']:%Y%m%dT%H%M%S}.parquet")
    bars = pl.DataFrame({"ingested_at": [start_utc] * 2, "symbol": ["SPY"] * 2,
                         "bar_start": [start_utc, start_utc + timedelta(minutes=5)], "close": [1.0] * 2})
    d = data / f"staged/equity_bars/ingest_date={day}"
    d.mkdir(parents=True, exist_ok=True)
    bars.write_parquet(d / "b.parquet")


def test_data_copy_adds_the_missing_days_and_leaves_base_and_production_alone(tmp_path, cases_dir):
    prod, fx = tmp_path / "data", tmp_path / "fx"
    write_runs(prod)  # 2026-10-26
    (prod / "staged/equity_bars").mkdir(parents=True)
    base = fx / "base"
    build_base(prod, base)
    assert last_day(base) == date(2026, 10, 26)
    # production grows after the freeze: 10-27, an evening run that lands in UTC 10-28, and 10-28 itself
    add_day(prod, date(2026, 10, 27), datetime(2026, 10, 27, 13, 30, tzinfo=timezone.utc))
    add_day(prod, date(2026, 10, 27), datetime(2026, 10, 28, 0, 30, tzinfo=timezone.utc), n=1)  # 20:30 ET 10-27
    add_day(prod, date(2026, 10, 28), datetime(2026, 10, 28, 13, 30, tzinfo=timezone.utc))
    before_base, before_prod = digest(base), digest(prod)

    case = drafts.draft(note(question="Why was 2026-10-27 slow?", category="hung_request", confirmed_on="2026-10-27"), 4)
    path, copy = drafts.write(case, cases_dir, prod, base, fx)
    assert digest(base) == before_base and digest(prod) == before_prod
    days = _read_runs(copy).select(pl.col("started_at").dt.convert_time_zone("America/New_York").dt.date())
    assert sorted(set(days.to_series().to_list())) == [date(2026, 10, 26), date(2026, 10, 27)]  # not 10-28
    assert days.height == 4 + 4  # base's 4 runs + 10-27's 3 + the evening run
    written = yaml.safe_load(path.read_text())
    assert written["own_data"] is True and written["label"]["status"] == "needs_review"
    assert json.loads((copy / "copy.json").read_text())["days"] == ["2026-10-27", "2026-10-27"]

    with pytest.raises(FileExistsError):  # neither the draft nor its copy is ever replaced
        drafts.write(case, cases_dir, prod, base, fx)
    with pytest.raises(FileExistsError):
        build_real_case(case["id"], date(2026, 10, 27), prod, base, fx)
    assert build_real_case("r99", date(2026, 10, 26), prod, base, fx) is None  # the base covers it
    assert not (fx / "r99").exists()


def test_eval_leaves_needs_review_out_of_the_score_and_memory(monkeypatch, tmp_path):
    checked = EXISTING | {"_kind": "real", "_file": "real/r03.yaml"}
    draft = drafts.draft(note(), 4) | {"_kind": "real", "_file": "real/r04.yaml"}
    monkeypatch.setattr(ev, "load_cases", lambda kinds=None: [checked, draft])
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    seen = []

    def fake_run(case, model, thinking, replay, record, log_dir, memory, pool, budget):
        seen.append((case["id"], [c["id"] for c in pool]))
        return {"id": case["id"], "kind": "real", "simulated": False, "status": "ok", "steps": 1, "run_id": None,
                "wall_s": 1.0, "compactions": 0, "memory_notes": 0, "rejections": [], "expected": case["expected"],
                "category": "x", "runbook": "y", "category_ok": True, "runbook_ok": True, "correct": True,
                "actions_requested": [], "actions_executed": [], "blocked": True, "model_refused": True,
                "error": None, "root_cause": ""}

    monkeypatch.setattr(ev, "run_case", fake_run)
    out = tmp_path / "r.json"
    assert ev.main(["--no-record", "--memory", "loo", "--out", str(out)]) == 0
    assert seen == [(EXISTING["id"], [EXISTING["id"]])]  # the draft neither ran nor went into memory
    s = json.loads(out.read_text())["summary"]
    assert s["n"] == 1 and "unreviewed" not in s

    seen.clear()
    assert ev.main(["--no-record", "--include-unreviewed", "--out", str(out)]) == 0
    assert [i for i, _ in seen] == [EXISTING["id"], draft["id"]]
    s = json.loads(out.read_text())["summary"]
    assert s["n"] == 1 and s["unreviewed"] == {"n": 1, "correct": 1, "ids": [draft["id"]]}
