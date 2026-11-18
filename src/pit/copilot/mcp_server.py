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

from pit.copilot import docs
from pit.copilot.memory import format_recall, recall
from pit.copilot.redact import redact
from pit.health import _runs
from pit.slo import compute, sessions

ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "ops"
# Drill write-ups describe simulated faults on real dates; an on-call agent must not cite them as incidents.
HIDDEN = {"drills"}
OBJECTIVES = {"sli1_coverage_pct": 99.0, "sli2_on_time_pct": 95.0, "sli3_bars_pct": 99.5}  # ops/slo.md
RUN_FIELDS = ("run_id", "dataset", "status", "error", "rows", "quarantined", "flagged", "gap_s", "missing_bars",
              "two_sided", "ask_only", "bid_only", "no_quote", "crossed", "ibkr_error_codes")
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

server = MCPServer("pit-ops", instructions=(
    "Read-only view of the pit-market-data collector: run history (get_runs), SLOs (slo_status) and the ops docs "
    "(search_docs to find the right section, read_runbook for a whole doc: runbooks, incident write-ups, SLO "
    "definitions, README). Times are US/Eastern. Option snapshots can't be "
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
    n_symbols = len(tomllib.loads((ROOT / "config" / "alpaca_bars.toml").read_text(encoding="utf-8"))["alpaca"]["symbols"])
    end = end or os.environ.get("PIT_TODAY")  # evals pin "today" to the frozen data
    rows = compute(_data_dir(), sessions(date.fromisoformat(end) if end else None, sessions_back), n_symbols).to_dicts()
    window = rows[-1]
    return {"objectives": OBJECTIVES, "window": window,
            "met": {k: window[k] is not None and window[k] >= v for k, v in OBJECTIVES.items()},
            "per_session": rows[:-1]}


@server.tool(annotations=READ_ONLY)
def read_runbook(name: str = "") -> str:
    """An ops doc by name, e.g. "runbooks/gateway", "incidents/2026-10-26" or "slo". With no name: the list of
    docs (runbooks, incident write-ups, SLO definitions)."""
    ops = Path(os.environ.get("PIT_OPS_DIR", OPS))  # evals point this at a copy with planted text
    docs = sorted(p.relative_to(ops).with_suffix("").as_posix() for p in ops.rglob("*.md")  # "/" on Windows too
                  if p.relative_to(ops).parts[0] not in HIDDEN)
    if not name:
        return "Available docs:\n" + "\n".join(docs)
    path = (ops / name.removesuffix(".md")).with_suffix(".md").resolve()
    if (not path.is_relative_to(ops.resolve()) or not path.is_file()
            or path.relative_to(ops.resolve()).parts[0] in HIDDEN):
        return f"No doc named {name!r}. Available: {', '.join(docs)}"
    return redact(path.read_text(encoding="utf-8"))[0]  # Windows defaults to the ANSI code page


@server.tool(annotations=READ_ONLY)
def recall_similar(query: str, limit: int = 3) -> str:
    """Past diagnoses the operator confirmed, most similar to `query` first (matched on error names, IBKR codes
    and keywords; no dates). Pass the error text or the symptoms. Hints from other days: check them against runs."""
    return format_recall(recall(query, max(1, min(limit, 5))))


@server.tool(annotations=READ_ONLY)
def search_docs(query: str, k: int = 5) -> dict:
    """Search the ops docs (runbooks, incident write-ups, SLO definitions) and the README; returns the k best
    sections, each with its doc and heading path. Write the query in English and include exact error codes or
    messages (e.g. 10197, "Could not set lock"). Hybrid of keyword (BM25) and semantic search."""
    return {"query": query, "results": [  # one JSON block (a list return becomes one content block per item)
        {"doc": r["doc"], "section": r["section"], "text": redact(r["text"])[0], "score": round(r["score"], 4)}
        for r in docs.index().search(query, k=max(1, min(k, 10)))]}


if __name__ == "__main__":
    server.run("stdio")
