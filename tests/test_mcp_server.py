import json
from datetime import datetime, timedelta, timezone

import anyio
import polars as pl
from mcp import Client

from pit.copilot.mcp_server import OPS, server
from pit.pipeline import RUN_SCHEMA

AT = datetime(2026, 10, 26, 13, 10, tzinfo=timezone.utc)  # 09:10 ET


def call(name, args=None):
    async def go():
        async with Client(server) as c:
            if name == "list_tools":
                return (await c.list_tools()).tools
            return (await c.call_tool(name, args or {})).content[0].text
    return anyio.run(go)


def write_runs(tmp_path, error=None):
    """Three failed option runs from 09:10 ET on 2026-10-26 and one good equity run. error: the failed runs' error."""
    error = error or "TimeoutError() for account U1234567"
    rows = [{"run_id": f"scheduled__{AT + i * timedelta(minutes=5)}", "dataset": "option_snapshots",
             "started_at": AT + i * timedelta(minutes=5), "finished_at": AT + i * timedelta(minutes=5, seconds=4),
             "status": "failed", "error": error} for i in range(3)]
    rows.append({"run_id": "scheduled__x", "dataset": "equity_bars", "started_at": AT, "finished_at": AT,
                 "status": "success", "rows": 3900, "missing_bars": 0})
    for ds in ("option_snapshots", "equity_bars"):  # the real layout: one partition per dataset and UTC date
        d = tmp_path / f"pipeline_runs/dataset={ds}/ingest_date=2026-10-26"
        d.mkdir(parents=True)
        pl.DataFrame([r for r in rows if r["dataset"] == ds], schema=RUN_SCHEMA).write_parquet(d / "runs.parquet")


def test_tools_are_read_only():
    tools = {t.name: t for t in call("list_tools")}
    assert set(tools) == {"get_runs", "slo_status", "read_runbook", "recall_similar"}  # memory: read-only too
    assert all(t.annotations.read_only_hint and not t.annotations.destructive_hint for t in tools.values())


def test_get_runs_filters_and_redacts(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    write_runs(tmp_path)
    out = json.loads(call("get_runs", {"status": "failed", "limit": 2}))   # default day: the latest with runs
    assert (out["day"], out["total"], out["failed"]) == ("2026-10-26", 3, 3)
    assert [r["started_et"] for r in out["runs"]] == ["09:20:00", "09:15:00"]   # newest first, ET
    assert out["runs"][0]["error"] == "TimeoutError() for account [IBKR_ACCOUNT]"
    assert json.loads(call("get_runs", {"day": "2026-10-26", "dataset": "equity_bars"}))["runs"][0]["rows"] == 3900


def test_read_runbook_lists_reads_and_stays_inside_ops():
    assert "runbooks/gateway" in call("read_runbook")
    assert call("read_runbook", {"name": "incidents/2026-10-26"}).startswith("# 2026-10-26")
    assert call("read_runbook", {"name": "../../.env"}).startswith("No doc named")
    assert call("read_runbook", {"name": "/etc/passwd"}).startswith("No doc named")


def test_read_runbook_hides_simulated_drills():
    assert (OPS / "drills").is_dir()  # the write-ups exist, but the agent can neither list nor read them
    assert "drills/" not in call("read_runbook")
    name = next((OPS / "drills").glob("*.md")).stem
    assert call("read_runbook", {"name": f"drills/{name}"}).startswith("No doc named")


def test_slo_status_reports_objectives(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    write_runs(tmp_path)
    b = tmp_path / "staged/equity_bars/ingest_date=2026-10-26"
    b.mkdir(parents=True)
    pl.DataFrame({"symbol": ["SPY"], "bar_start": [AT]}, schema={"symbol": pl.String, "bar_start": pl.Datetime("us", "UTC")}) \
        .write_parquet(b / "bars.parquet")
    out = json.loads(call("slo_status", {"end": "2026-10-26", "sessions_back": 1}))
    assert out["window"]["sli1_coverage_pct"] == 0.0 and out["met"]["sli1_coverage_pct"] is False
    assert out["objectives"]["sli1_coverage_pct"] == 99.0 and len(out["per_session"]) == 1
