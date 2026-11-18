"""HTTP API for the PIT Ops Copilot: POST /ask {"question": ...} runs the same agent loop as the CLI and returns
the diagnosis, the tool calls it made, tokens, cost and latency.

- Auth: every /ask needs `Authorization: Bearer <PIT_API_TOKEN>` (from .env). With no token configured, /ask
  answers 503: the API never runs open. GET /health needs no token and says nothing about the data.
- Risky tools never run: there is no terminal to approve at, so the approver is deny_all, and
  PIT_COPILOT_NO_ACTIONS=1 is set for the process as a second lock. A request for one shows up in
  `actions_requested`, denied.
- Memory is read-only here: the API never asks "confirm?", so nothing it answers is saved.
- Cost: every call goes through pit.copilot.llm, so it is logged in data/llm_calls/ and refused once
  config/llm.toml's daily_budget_usd is spent (429). One diagnosis at a time; others wait.

  uv run --env-file .env uvicorn pit.copilot.api:app --host 127.0.0.1 --port 8000
  curl -s -H "Authorization: Bearer $PIT_API_TOKEN" -H "Content-Type: application/json" \\
       -d '{"question": "Why were there no option snapshots on 2026-10-26?"}' http://127.0.0.1:8000/ask
"""

import os
import secrets
import time
from collections.abc import Callable
from pathlib import Path

import anyio
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from pit.copilot.actions import deny_all
from pit.copilot.agent import diagnose, make_llm
from pit.copilot.llm import LLM, BudgetExceeded, read_log

MIN_TOKEN_CHARS = 16


class Ask(BaseModel):
    question: str = Field(min_length=3, max_length=2000)


def _rows(directory: Path, run_id: str) -> list[dict]:
    """This run's rows from a JSONL log directory (today's and yesterday's UTC files: a run can cross midnight)."""
    files = sorted(directory.glob("*.jsonl"))[-2:] if directory.is_dir() else []
    return [r for f in files for r in read_log(f) if run_id in (r.get("run_id") or r.get("purpose") or "")]


def _root(e: BaseException) -> BaseException:
    """The first real error inside exception groups (the MCP client's task group wraps what the loop raised)."""
    while isinstance(e, BaseExceptionGroup) and e.exceptions:
        e = e.exceptions[0]
    return e


def usage(llm_calls: list[dict]) -> dict:
    return {"llm_calls": len(llm_calls),
            "tokens": {"prompt": sum(r.get("prompt_tokens") or 0 for r in llm_calls),
                       "prompt_cache_hit": sum(r.get("cache_hit_tokens") or 0 for r in llm_calls),
                       "completion": sum(r.get("completion_tokens") or 0 for r in llm_calls)},
            "cost_usd": round(sum(r.get("cost_usd") or 0 for r in llm_calls), 6),
            "llm_latency_s": round(sum(r.get("latency_s") or 0 for r in llm_calls), 3)}


def create_app(llm_factory: Callable[[], LLM] = make_llm, data_dir: Path | None = None,
               token: str | None = None) -> FastAPI:
    """llm_factory: a fresh LLM per request (tests pass one with a scripted client). token: defaults to
    PIT_API_TOKEN."""
    os.environ["PIT_COPILOT_NO_ACTIONS"] = "1"
    os.environ.setdefault("PIT_AGENT_QUIET", "1")
    app = FastAPI(title="PIT Ops Copilot", version="1", description=__doc__.split("\n")[0])
    lock = anyio.Lock()

    def data() -> Path:
        return Path(data_dir or os.environ.get("DATA_DIR", "data"))

    def check_token(request: Request) -> None:
        want = token if token is not None else os.environ.get("PIT_API_TOKEN", "")
        if len(want) < MIN_TOKEN_CHARS:
            raise HTTPException(503, f"PIT_API_TOKEN is not set (at least {MIN_TOKEN_CHARS} characters); /ask is off")
        scheme, _, got = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(got.strip().encode(), want.encode()):
            raise HTTPException(401, "missing or wrong bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/health")
    def health() -> dict:
        return {"ok": True}

    @app.post("/ask", dependencies=[Depends(check_token)])
    async def ask(body: Ask) -> dict:
        llm = llm_factory()

        def run() -> dict:  # the LLM client blocks: run the loop on a worker thread with its own event loop
            return anyio.run(lambda: diagnose(body.question, llm, purpose="api", approve=deny_all,
                                              trace_dir=data() / "copilot_traces"))

        async with lock:
            t0 = time.monotonic()
            try:
                out = await anyio.to_thread.run_sync(run)
            except Exception as e:
                e = _root(e)
                if isinstance(e, BudgetExceeded):
                    raise HTTPException(429, str(e)) from None
                # the model API or a tool failed: say which, without a stack trace
                raise HTTPException(502, f"diagnosis failed: {type(e).__name__}: {str(e)[:200]}") from None
            wall = round(time.monotonic() - t0, 3)
        trace = _rows(data() / "copilot_traces", out["run_id"])
        tool_calls = [{"step": r["step"], "tool": r["tool"], "args": r.get("args"),
                       "result_chars": r.get("result_chars"), "latency_s": r.get("latency_s")}
                      for r in trace if r.get("kind") == "tool"]
        return {"run_id": out["run_id"], "status": out["status"], "diagnosis": out["diagnosis"],
                "model": out["model"], "steps": out["steps"], "tool_calls": tool_calls,
                "actions_requested": [{"tool": a["tool"], "reason": a["reason"], "approved": a["approved"]}
                                      for a in out["actions"]],
                "rejections": out.get("rejections") or [], "compactions": out.get("compactions") or 0,
                **usage(_rows(llm.log_dir, out["run_id"])), "wall_s": wall}

    return app


def __getattr__(name):  # `uvicorn pit.copilot.api:app` builds the app on first use, not on import (tests)
    if name == "app":
        return create_app()
    raise AttributeError(name)
