"""MCP server for the PIT Ops Copilot: read-only tools over the pipeline's run log, SLOs and ops docs.

One set of tools for the Copilot's own agent loop and for any MCP host. Claude Code picks it up from .mcp.json at
the repo root; by hand: uv run python -m pit.copilot.mcp_server (stdio). Nothing here writes, restarts or calls
IBKR. Text going out is redacted (pit.copilot.redact), since run errors can quote account data.
"""

import os
from datetime import date
from pathlib import Path

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from pit.copilot.redact import redact
from pit.health import _runs
from pit.slo import compute, sessions

ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "ops"
OBJECTIVES = {"sli1_coverage_pct": 99.0, "sli2_on_time_pct": 95.0, "sli3_bars_pct": 99.5}  # ops/slo.md
RUN_FIELDS = ("run_id", "dataset", "status", "error", "rows", "quarantined", "flagged", "gap_s", "missing_bars",
              "two_sided", "ask_only", "bid_only", "no_quote", "crossed", "ibkr_error_codes")
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

server = MCPServer("pit-ops", instructions=(
    "Read-only view of the pit-market-data collector: run history (get_runs), SLOs (slo_status) and the ops docs "
    "(read_runbook: runbooks, incident write-ups, SLO definitions). Times are US/Eastern. Option snapshots can't be "
    "backfilled, so a failed option slot is permanent data loss."))


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", ROOT / "data"))


def _clean(v):
    if isinstance(v, str):
        return redact(v)[0]
    return v.isoformat() if hasattr(v, "isoformat") else v


@server.tool(annotations=READ_ONLY)
def get_runs(day: str | None = None, dataset: str | None = None, status: str | None = None, limit: int = 50) -> dict:
    """Collection runs on one New York day from pipeline_runs, newest first.

    day: YYYY-MM-DD (default: the latest day with runs). dataset: option_snapshots, index_snapshots, equity_bars,
    daily_bars, corporate_actions or assets. status: success or failed. Each run has its ET start time, duration,
    error, row counts, option quote_status counts and IBKR error codes.
    """
    root = _data_dir() / "pipeline_runs"
    days = [date.fromisoformat(day)] if day else sorted(
        {date.fromisoformat(p.name.split("=")[1]) for p in root.glob("dataset=*/ingest_date=*")}, reverse=True)[:2]
    for d in days:  # a partition is a UTC date; the latest NY day is in it or the one before
        runs = _runs(_data_dir(), d)
        if not runs.is_empty():
            break
    else:
        return {"day": day, "runs": [], "note": "no runs recorded"}
    if dataset:
        runs = runs.filter(runs["dataset"] == dataset)
    if status:
        runs = runs.filter(runs["status"] == status)
    runs = runs.sort("started_at", descending=True)
    out = [{"started_et": r["et"].strftime("%H:%M:%S"), "duration_s": r["run_s"],
            **{k: _clean(r.get(k)) for k in RUN_FIELDS if r.get(k) is not None}}
           for r in runs.head(limit).iter_rows(named=True)]
    return {"day": str(d), "total": runs.height, "failed": int((runs["status"] == "failed").sum()), "runs": out}


@server.tool(annotations=READ_ONLY)
def slo_status(end: str | None = None, sessions_back: int = 20) -> dict:
    """The three SLIs of ops/slo.md (option coverage, option timeliness, equity bar completeness) per session and
    over the window, with the objectives and whether each is met.

    end: last session YYYY-MM-DD (default: today). sessions_back: window length in sessions.
    """
    import tomllib
    n_symbols = len(tomllib.loads((ROOT / "config" / "alpaca_bars.toml").read_text())["alpaca"]["symbols"])
    rows = compute(_data_dir(), sessions(date.fromisoformat(end) if end else None, sessions_back), n_symbols).to_dicts()
    window = rows[-1]
    return {"objectives": OBJECTIVES, "window": window,
            "met": {k: window[k] is not None and window[k] >= v for k, v in OBJECTIVES.items()},
            "per_session": rows[:-1]}


@server.tool(annotations=READ_ONLY)
def read_runbook(name: str = "") -> str:
    """An ops doc by name, e.g. "runbooks/gateway", "incidents/2026-10-26" or "slo". With no name: the list of
    docs (runbooks, incident write-ups, SLO definitions)."""
    docs = sorted(str(p.relative_to(OPS).with_suffix("")) for p in OPS.rglob("*.md"))
    if not name:
        return "Available docs:\n" + "\n".join(docs)
    path = (OPS / name.removesuffix(".md")).with_suffix(".md").resolve()
    if not path.is_relative_to(OPS.resolve()) or not path.is_file():
        return f"No doc named {name!r}. Available: {', '.join(docs)}"
    return redact(path.read_text())[0]


if __name__ == "__main__":
    server.run("stdio")
