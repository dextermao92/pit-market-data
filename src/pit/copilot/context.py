"""Context budget for the agent loop: keep each LLM call's prompt under config/llm.toml's context_budget_tokens.

The prompt grows by every tool result (the whole conversation is sent on each step). When the next call is
estimated to go over the budget, older tool results are replaced by short stubs: tool name, arguments, the key
fields and a one-line summary, made by fixed rules from the full result (no LLM call). Tool results from the latest
round stay whole. A replaced result stays replaced, and compaction goes down to COMPACT_TO of the budget, so it
happens rarely: every compaction changes the prompt prefix and costs DeepSeek's prompt cache from that point on.

Token estimate: the previous call's reported prompt + completion tokens, plus new text at CHARS_PER_TOKEN, a
conservative (low) ratio from the 2026-11-12 eval traces (median 2.84 characters per token, minimum 2.42).
"""

import itertools
import json

CHARS_PER_TOKEN = 2.4
COMPACT_TO = 0.75  # compact down to this share of the budget, not just under it
MAX_ERRORS = 5     # distinct errors listed in a get_runs stub
ERROR_CHARS = 120


def tokens(text: str) -> int:
    return int(len(text) / CHARS_PER_TOKEN) + 1


def message_text(m: dict) -> str:
    """Everything of a message that the API bills as prompt."""
    parts = [m.get("content") or "", m.get("reasoning_content") or ""]
    parts += [c["function"]["name"] + (c["function"]["arguments"] or "") for c in m.get("tool_calls") or []]
    return "".join(parts)


def _head(text: str, n: int = 300) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[:n] + " ..."


def _secs(hms: str) -> int | None:
    try:
        h, m, sec = (int(x) for x in hms.split(":"))
        return h * 3600 + m * 60 + sec
    except (ValueError, AttributeError):
        return None


def _hms(secs: int) -> str:
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def _runs_stub(text: str) -> str:
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        return _head(text)
    runs = d.get("runs") or []
    if not runs:
        return f"day {d.get('day')}: no runs ({d.get('note', '')})".strip()
    by_ds = {}
    for r in runs:
        ds = by_ds.setdefault(r.get("dataset"), {"success": 0, "failed": 0})
        ds[r.get("status")] = ds.get(r.get("status"), 0) + 1
    times = sorted(r["started_et"] for r in runs if r.get("started_et"))
    failed = [r for r in runs if r.get("status") == "failed"]
    codes = sorted({str(c) for r in runs for c in (r.get("ibkr_error_codes") or [])})
    lines = [(f"day {d.get('day')}: {d.get('total')} runs, {d.get('failed')} failed; {len(runs)} shown, "
             f"{times[0]}-{times[-1]} ET"),
             "shown by dataset: " + ", ".join(f"{k} {v['success']} ok / {v['failed']} failed"
                                              for k, v in sorted(by_ds.items(), key=lambda kv: str(kv[0])))]
    durs = sorted((r["duration_s"], r.get("started_et"), r.get("dataset")) for r in runs
                  if isinstance(r.get("duration_s"), (int, float)))
    if durs:
        d_max = durs[-1]
        lines.append(f"duration_s: median {durs[len(durs) // 2][0]}, max {d_max[0]} ({d_max[2]} at {d_max[1]} ET)")
    secs = sorted(_secs(t) for t in times if _secs(t) is not None)
    if len(secs) > 1:
        gap, at = max((b - a, a) for a, b in itertools.pairwise(secs))
        lines.append(f"largest gap between shown runs: {gap // 60} min after {_hms(at)} ET")
    if failed:  # grouped by error text: a stalled morning repeats one error dozens of times
        groups = {}
        for r in failed:
            g = groups.setdefault((r.get("error") or "")[:ERROR_CHARS], {"n": 0, "datasets": set(), "times": []})
            g["n"] += 1
            g["datasets"].add(str(r.get("dataset")))
            g["times"].append(r.get("started_et") or "")
        top = sorted(groups.items(), key=lambda kv: -kv[1]["n"])[:MAX_ERRORS]
        lines.append(f"failed runs by error ({len(groups)} distinct):")
        lines += [f"- {g['n']}x {'/'.join(sorted(g['datasets']))} {min(g['times'])}-{max(g['times'])} ET: {err}"
                  for err, g in top]
        if len(groups) > MAX_ERRORS:
            lines.append(f"- +{len(groups) - MAX_ERRORS} more distinct errors")
    if codes:
        lines.append("IBKR error codes: " + ", ".join(codes))
    return "\n".join(lines)


def _doc_stub(text: str) -> str:
    lines = text.splitlines()
    title = next((x.lstrip("# ").strip() for x in lines if x.startswith("#")), _head(text, 120))
    heads = [x.lstrip("# ").strip() for x in lines if x.startswith("## ")]
    return f"{title} ({len(text)} chars)" + (f"; sections: {', '.join(heads)}" if heads else "")


def _slo_stub(text: str) -> str:
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        return _head(text)
    return json.dumps({"window": d.get("window"), "met": d.get("met")}, default=str)


def stub(name: str, args: dict | None, text: str) -> str:
    """A short stand-in for a tool result, made from the full result by fixed rules."""
    if text.startswith(("Available docs:", "Error")) or len(text) < 400:
        body = text  # already short
    elif name == "get_runs":
        body = _runs_stub(text)
    elif name == "read_runbook":
        body = _doc_stub(text)
    elif name == "slo_status":
        body = _slo_stub(text)
    else:
        body = _head(text)
    return (f"[compacted to save context: {name}({json.dumps(args or {}, sort_keys=True)}), {len(text)} chars. "
            f"Call the tool again if you need the detail.]\n{body}")


class Budget:
    """Tracks the prompt size of an agent run and compacts older tool results to keep it under `limit` tokens."""

    def __init__(self, limit: int):
        self.limit = limit  # 0: no budget
        self.stubs: dict[str, str] = {}  # tool_call_id -> stub, made when the result came in
        self.known_tokens = 0  # prompt + completion of the last call
        self.known_upto = 0    # messages covered by known_tokens

    def add_stub(self, call_id: str, name: str, args: dict | None, text: str) -> None:
        if self.limit:
            self.stubs[call_id] = stub(name, args, text)

    def observed(self, usage, n_messages: int) -> None:
        """After a call: its reported usage covers the first n_messages messages (incl. the reply)."""
        if usage is not None:
            self.known_tokens = (getattr(usage, "prompt_tokens", 0) or 0) + (getattr(usage, "completion_tokens", 0) or 0)
            self.known_upto = n_messages

    def estimate(self, messages: list[dict]) -> int:
        if not self.known_upto:
            return sum(tokens(message_text(m)) for m in messages)
        return self.known_tokens + sum(tokens(message_text(m)) for m in messages[self.known_upto:])

    def enforce(self, messages: list[dict]) -> dict | None:
        """Compact in place if the next call would go over the limit. Returns what was done (for the trace)."""
        if not self.limit:
            return None
        before = self.estimate(messages)
        if before <= self.limit:
            return None
        last_assistant = max(i for i, m in enumerate(messages) if m.get("role") == "assistant")
        target, est, done = int(self.limit * COMPACT_TO), before, []
        for i, m in enumerate(messages[:last_assistant]):  # oldest first; the latest round stays whole
            if est <= target:
                break
            sid = m.get("tool_call_id")
            if m.get("role") != "tool" or sid not in self.stubs or m["content"] == self.stubs[sid]:
                continue
            saved = tokens(m["content"]) - tokens(self.stubs[sid])
            if saved <= 0:
                continue
            done.append({"tool_call_id": sid, "chars_before": len(m["content"]), "chars_after": len(self.stubs[sid])})
            messages[i] = m | {"content": self.stubs[sid]}
            est -= saved
            self.known_tokens -= saved if i < self.known_upto else 0
        return {"estimate_before": before, "estimate_after": est, "limit": self.limit, "compacted": done,
                "still_over": est > self.limit}

