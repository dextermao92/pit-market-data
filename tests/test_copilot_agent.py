import json
from types import SimpleNamespace

import anyio
import pytest

from pit.copilot.agent import assistant_turn, check_diagnosis, diagnose
from pit.copilot.llm import LLM, load_config
from test_mcp_server import write_runs  # pytest puts tests/ on sys.path (no __init__.py)

CFG = load_config()


def call(name, args, id_="c1"):
    return SimpleNamespace(id=id_, type="function", function=SimpleNamespace(name=name, arguments=json.dumps(args)))


def reply(*calls, content="", reasoning=None):
    msg = SimpleNamespace(content=content, tool_calls=list(calls) or None)
    if reasoning:
        msg.reasoning_content = reasoning
    usage = SimpleNamespace(prompt_tokens=100, completion_tokens=20, prompt_cache_hit_tokens=0)
    return SimpleNamespace(id="req", usage=usage,
                           choices=[SimpleNamespace(message=msg, finish_reason="tool_calls" if calls else "stop")])


class Scripted:
    """A fake OpenAI client that plays back replies in order and keeps what was sent."""

    def __init__(self, *replies):
        self.replies, self.sent, self.tools = list(replies), [], []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, **kw):
        self.sent.append(json.loads(json.dumps(kw["messages"], default=vars)))
        self.tools.append([t["function"]["name"] for t in kw.get("tools", [])])
        return self.replies.pop(0)


GOOD = {"root_cause": "IB Gateway API connect timed out", "category": "gateway_frozen",
        "runbook": "runbooks/gateway",
        "evidence": [{"day": "2026-10-26", "time_et": "09:10", "dataset": "option_snapshots",
                      "detail": "TimeoutError()"}]}


def run(client, tmp_path, monkeypatch, thinking=False, max_steps=8, error=None, **kw):
    """kw goes to diagnose (approve, send_command). error: the failed runs' error text (write_runs)."""
    write_runs(tmp_path, error)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PIT_AGENT_QUIET", "1")
    cfg = CFG | {"llm": CFG["llm"] | {"thinking": thinking}}
    llm = LLM(cfg=cfg, client=client, log_dir=tmp_path / "llm_calls")
    return anyio.run(lambda: diagnose("Why no option snapshots on 2026-10-26 morning?", llm, max_steps,
                                      trace_dir=tmp_path / "traces", today="2026-11-12", **kw))


def trace_rows(tmp_path):
    return [json.loads(line) for f in (tmp_path / "traces").glob("*.jsonl") for line in f.read_text().splitlines()]


def test_tool_then_submit(tmp_path, monkeypatch):
    client = Scripted(reply(call("get_runs", {"day": "2026-10-26", "status": "failed"})),
                      reply(call("submit_diagnosis", GOOD, "c2")))
    out = run(client, tmp_path, monkeypatch)

    assert out["status"] == "ok" and out["steps"] == 2 and out["diagnosis"] == GOOD
    tool_msg = client.sent[1][-1]
    assert tool_msg["role"] == "tool" and tool_msg["tool_call_id"] == "c1"
    runs = json.loads(tool_msg["content"])
    assert runs["failed"] == 3 and "[IBKR_ACCOUNT]" in runs["runs"][0]["error"]  # redacted before the LLM sees it
    kinds = [r["kind"] for r in trace_rows(tmp_path)]
    assert kinds == ["question", "llm", "tool", "llm", "tool", "final"]
    assert len(list((tmp_path / "llm_calls").glob("*.jsonl"))) == 1  # cost log written by pit.copilot.llm
    assert "search_docs" not in client.tools[0] and "read_runbook" in client.tools[0]  # retrieval: Claude Code only


def test_bad_runbook_is_rejected_and_retried(tmp_path, monkeypatch):
    client = Scripted(reply(call("submit_diagnosis", GOOD | {"runbook": "runbooks/made-up"})),
                      reply(call("submit_diagnosis", GOOD, "c2")))
    out = run(client, tmp_path, monkeypatch)
    assert out["status"] == "ok" and out["steps"] == 2
    assert client.sent[1][-1]["content"].startswith("Rejected: unknown runbook")


def test_max_steps_ends_without_a_guess(tmp_path, monkeypatch):
    client = Scripted(*[reply(call("read_runbook", {}, f"c{i}")) for i in range(3)])
    out = run(client, tmp_path, monkeypatch, max_steps=3)
    assert out == out | {"status": "max_steps", "diagnosis": None, "steps": 3}
    assert client.sent[-1][-1] == {"role": "user", "content": "Last step: call submit_diagnosis now."}


def test_prose_answer_is_nudged(tmp_path, monkeypatch):
    client = Scripted(reply(content="It was the Gateway."), reply(call("submit_diagnosis", GOOD)))
    out = run(client, tmp_path, monkeypatch)
    assert out["status"] == "ok"
    assert client.sent[1][-1]["content"] == "Submit your answer with submit_diagnosis."


def test_unknown_tool_and_bad_json_go_back_as_errors(tmp_path, monkeypatch):
    bad_json = SimpleNamespace(id="c2", type="function", function=SimpleNamespace(name="get_runs", arguments="{"))
    client = Scripted(reply(call("restart_gateway", {}), bad_json), reply(call("submit_diagnosis", GOOD, "c3")))
    run(client, tmp_path, monkeypatch)
    results = [m["content"] for m in client.sent[1] if m["role"] == "tool"]
    assert results[0] == "Error: no tool named 'restart_gateway'" and results[1].startswith("Error: arguments")


def test_thinking_mode_sends_reasoning_back(tmp_path, monkeypatch):
    client = Scripted(reply(call("read_runbook", {"name": "runbooks/gateway"}), reasoning="check the runbook"),
                      reply(call("submit_diagnosis", GOOD, "c2")))
    run(client, tmp_path, monkeypatch, thinking=True)
    turn = client.sent[1][-2]
    assert turn["role"] == "assistant" and turn["reasoning_content"] == "check the runbook"
    assert "reasoning_content" not in assistant_turn(SimpleNamespace(content="x", tool_calls=None))


@pytest.mark.parametrize("d, ok", [(GOOD, True), (GOOD | {"evidence": []}, False), (GOOD | {"category": "x"}, False),
                                   (GOOD | {"runbook": "runbooks/gateway.md"}, True)])
def test_check_diagnosis(d, ok):
    assert (check_diagnosis(d, {"runbooks/gateway", "slo"}) is None) == ok
