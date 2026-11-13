"""LLM client for the Copilot: DeepSeek's OpenAI-compatible API. Every call is logged with tokens, cost and latency.

Each call appends one JSON line to data/llm_calls/<UTC date>.jsonl: usage and timing only, never the prompt or the
answer (those can hold account data). Outgoing message text is redacted first (pit.copilot.redact).

Usage (the key comes from .env):
  uv run --env-file .env python -m pit.copilot.llm "question"   one call: the answer, then its log row
  uv run python -m pit.copilot.llm --report                     calls, tokens, cost, latency per day and model
"""

import json
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pit.copilot.redact import redact

CONFIG = Path(__file__).resolve().parents[3] / "config" / "llm.toml"


class BudgetExceeded(RuntimeError):
    pass


def load_config(path: Path = CONFIG) -> dict:
    return tomllib.loads(Path(path).read_text())


def is_peak(at: datetime, cfg: dict) -> bool:
    at = at.astimezone(timezone.utc)
    return at.weekday() < 5 and any(lo <= at.hour < hi for lo, hi in cfg["peak"]["hours"])


def usage_of(u) -> dict:
    hit = getattr(u, "prompt_cache_hit_tokens", None)  # DeepSeek's field; cached_tokens is the OpenAI-style copy
    if hit is None:
        hit = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", None) or 0
    return {"prompt_tokens": u.prompt_tokens, "cache_hit_tokens": hit, "cache_miss_tokens": u.prompt_tokens - hit,
            "completion_tokens": u.completion_tokens,
            "reasoning_tokens": getattr(getattr(u, "completion_tokens_details", None), "reasoning_tokens", None) or 0}


def cost_usd(usage: dict, model: str, at: datetime, cfg: dict) -> float:
    p, i = cfg["prices"][model], int(is_peak(at, cfg))
    return (usage["cache_hit_tokens"] * p["cache_hit"][i] + usage["cache_miss_tokens"] * p["cache_miss"][i]
            + usage["completion_tokens"] * p["output"][i]) / 1e6


def redact_messages(messages: list) -> tuple[list, int]:
    """Copies with string contents masked. SDK message objects (the model's own turns) pass through as they are."""
    out, total = [], 0
    for m in messages:
        if isinstance(m, dict) and isinstance(m.get("content"), str):
            text, n = redact(m["content"])
            m, total = m | {"content": text}, total + n
        out.append(m)
    return out, total


class LLM:
    def __init__(self, cfg: dict | None = None, client=None, log_dir: Path | None = None):
        self.cfg = cfg or load_config()
        c = self.cfg["llm"]
        self.model, self.thinking = c["model"], c["thinking"]
        self.log_dir = Path(log_dir or Path(os.environ.get("DATA_DIR", "data")) / "llm_calls")
        if client is None:
            from openai import OpenAI
            client = OpenAI(api_key=os.environ[c["api_key_env"]], base_url=c["base_url"], timeout=c["timeout_seconds"])
        self.client = client

    def spent_today(self) -> float:
        path = self.log_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
        if not path.exists():
            return 0.0
        return sum(json.loads(line).get("cost_usd") or 0.0 for line in path.read_text().splitlines())

    def chat(self, messages: list, purpose: str, tools: list | None = None, **kwargs):
        """One chat completion; returns the SDK response. purpose labels the call in the log (e.g. "ask", "eval")."""
        budget = self.cfg["llm"]["daily_budget_usd"]
        if self.spent_today() >= budget:
            raise BudgetExceeded(f"today's LLM spend reached the {budget} USD budget (config/llm.toml)")
        messages, redactions = redact_messages(messages)
        at, t0 = datetime.now(timezone.utc), time.monotonic()
        if (temperature := self.cfg["llm"].get("temperature")) is not None:
            kwargs = {"temperature": temperature} | kwargs
        row = {"at": at.isoformat(), "purpose": purpose, "model": self.model, "thinking": self.thinking,
               "temperature": kwargs.get("temperature"), "tools": len(tools or []), "redactions": redactions}
        try:
            resp = self.client.chat.completions.create(
                model=self.model, messages=messages, **({"tools": tools} if tools else {}),
                extra_body={"thinking": {"type": "enabled" if self.thinking else "disabled"}}, **kwargs)
        except Exception as e:
            self._log(row | {"status": "error", "latency_s": round(time.monotonic() - t0, 3), "error": repr(e)[:300]})
            raise
        usage = usage_of(resp.usage)
        choice = resp.choices[0]
        self._log(row | {"status": "ok", "latency_s": round(time.monotonic() - t0, 3), "request_id": resp.id,
                         "finish_reason": choice.finish_reason, "tool_calls": len(choice.message.tool_calls or []),
                         **usage, "peak": is_peak(at, self.cfg), "cost_usd": cost_usd(usage, self.model, at, self.cfg)})
        return resp

    def _log(self, row: dict) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        with open(self.log_dir / f"{row['at'][:10]}.jsonl", "a") as f:
            f.write(json.dumps(row) + "\n")


def report(log_dir: Path) -> pl.DataFrame:
    calls = pl.concat([pl.read_ndjson(f) for f in sorted(log_dir.glob("*.jsonl"))], how="diagonal_relaxed")
    return (calls.with_columns(day=pl.col("at").str.slice(0, 10))
            .group_by("day", "model").agg(
                calls=pl.len(), errors=(pl.col("status") == "error").sum(),
                prompt_tokens=pl.col("prompt_tokens").sum(), cache_hit_tokens=pl.col("cache_hit_tokens").sum(),
                completion_tokens=pl.col("completion_tokens").sum(), cost_usd=pl.col("cost_usd").sum().round(6),
                latency_p50_s=pl.col("latency_s").median(), latency_p95_s=pl.col("latency_s").quantile(0.95))
            .sort("day", "model"))


if __name__ == "__main__":
    if sys.argv[1:] == ["--report"]:
        with pl.Config(tbl_cols=-1, tbl_width_chars=160):
            print(report(Path(os.environ.get("DATA_DIR", "data")) / "llm_calls"))
    else:
        llm = LLM()
        resp = llm.chat([{"role": "user", "content": " ".join(sys.argv[1:])}], purpose="cli")
        print(resp.choices[0].message.content)
        print((llm.log_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl").read_text().splitlines()[-1])
