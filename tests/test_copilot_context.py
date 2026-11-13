"""Context budget: older tool results become stubs made by fixed rules; the latest round stays whole."""

import json
from types import SimpleNamespace

from test_copilot_agent import GOOD, Scripted, call, reply, run, trace_rows

from pit.copilot.context import Budget, stub, tokens

RUNS = json.dumps({"day": "2026-10-26", "total": 120, "failed": 7, "runs": [
    {"started_et": f"09:{m:02d}:01", "duration_s": 4, "dataset": "option_snapshots", "status": "failed",
     "error": "TimeoutError() while connecting to 127.0.0.1:4004 " + "x" * 300, "ibkr_error_codes": [1100, 2103]}
    for m in range(16, 9, -1)] + [  # newest first, as get_runs returns them
    {"started_et": "09:05:00", "duration_s": 60, "dataset": "equity_bars", "status": "success", "rows": 3900}] * 40},
    indent=2)
DOC = "# Gateway runbook\n\nIntro text.\n\n## Symptoms\n" + "a line\n" * 200 + "## Fix\nRestart.\n"


def test_runs_stub_keeps_the_key_fields_and_is_short():
    s = stub("get_runs", {"day": "2026-10-26"}, RUNS)
    assert len(s) < len(RUNS) / 5
    assert s.startswith('[compacted to save context: get_runs({"day": "2026-10-26"})')
    assert "120 runs, 7 failed" in s and "- 7x option_snapshots 09:10:01-09:16:01 ET: TimeoutError()" in s
    assert "(1 distinct)" in s and "1100, 2103" in s and "equity_bars 40 ok / 0 failed" in s
    assert "duration_s: median 60, max 60" in s and "largest gap between shown runs: 5 min after 09:05:00 ET" in s


def test_doc_stub_keeps_title_and_sections():
    s = stub("read_runbook", {"name": "runbooks/gateway"}, DOC)
    assert "Gateway runbook" in s and "sections: Symptoms, Fix" in s and "a line" not in s


def test_short_results_are_kept_as_they_are():
    assert stub("read_runbook", {}, "Available docs:\nslo").endswith("Available docs:\nslo")


def convo(*tool_texts):
    """system, user, then one assistant turn + tool result per text; returns messages and a Budget with stubs."""
    b, msgs = Budget(limit=1), [{"role": "system", "content": "s"}, {"role": "user", "content": "q"}]
    for i, text in enumerate(tool_texts):
        msgs.append({"role": "assistant", "content": "", "tool_calls": [
            {"id": f"c{i}", "type": "function", "function": {"name": "get_runs", "arguments": "{}"}}]})
        msgs.append({"role": "tool", "tool_call_id": f"c{i}", "content": text})
        b.add_stub(f"c{i}", "get_runs", {}, text)
    return msgs, b


def test_compacts_oldest_first_down_to_target_and_keeps_latest_round():
    msgs, b = convo(RUNS, RUNS, RUNS)
    b.limit = tokens(RUNS) * 2  # three results don't fit, two do
    done = b.enforce(msgs)
    assert [x["tool_call_id"] for x in done["compacted"]] == ["c0", "c1"]  # 75 % target needs both older ones
    assert msgs[3]["content"].startswith("[compacted") and msgs[5]["content"].startswith("[compacted")
    assert msgs[7]["content"] == RUNS  # the latest round stays whole
    assert done["estimate_after"] < done["estimate_before"] and not done["still_over"]
    assert b.enforce(msgs) is None  # now under the limit: nothing more to do


def test_never_compacts_the_latest_round_even_when_over():
    msgs, b = convo(RUNS)
    b.limit = 10
    done = b.enforce(msgs)
    assert done["compacted"] == [] and done["still_over"] and msgs[-1]["content"] == RUNS


def test_no_budget_means_no_stubs_and_no_compaction():
    msgs, b = convo(RUNS, RUNS)
    b.limit, b.stubs = 0, {}
    assert b.enforce(msgs) is None and msgs[3]["content"] == RUNS


def test_estimate_uses_the_reported_usage_then_counts_new_text():
    msgs, b = convo(RUNS)
    b.observed(SimpleNamespace(prompt_tokens=5000, completion_tokens=50), n_messages=3)  # up to the assistant turn
    assert b.estimate(msgs) == 5050 + tokens(RUNS)


def big(prompt_tokens, *calls):
    r = reply(*calls)
    r.usage = SimpleNamespace(prompt_tokens=prompt_tokens, completion_tokens=20, prompt_cache_hit_tokens=0)
    return r


def test_agent_compacts_and_traces_it(tmp_path, monkeypatch):
    client = Scripted(big(2000, call("get_runs", {"day": "2026-10-26"}, "c1")),
                      big(9000, call("read_runbook", {"name": "runbooks/gateway"}, "c2")),
                      big(9500, call("submit_diagnosis", GOOD, "c3")))
    out = run(client, tmp_path, monkeypatch, context_budget=9000)
    assert out["status"] == "ok" and out["compactions"] == 1
    third = client.sent[2]
    first_result = next(m for m in third if m.get("tool_call_id") == "c1")
    assert first_result["content"].startswith("[compacted to save context: get_runs")
    assert not next(m for m in third if m.get("tool_call_id") == "c2")["content"].startswith("[compacted")
    rows = [r for r in trace_rows(tmp_path) if r["kind"] == "compact"]
    assert len(rows) == 1 and rows[0]["step"] == 3 and rows[0]["compacted"][0]["tool"] == "get_runs"
    assert rows[0]["compacted"][0]["args"] == {"day": "2026-10-26"}


def test_agent_without_budget_sends_everything(tmp_path, monkeypatch):
    client = Scripted(big(2000, call("get_runs", {"day": "2026-10-26"}, "c1")),
                      big(90000, call("submit_diagnosis", GOOD, "c2")))
    out = run(client, tmp_path, monkeypatch, context_budget=0)
    assert out["compactions"] == 0
    assert not any(str(m.get("content", "")).startswith("[compacted") for m in client.sent[1])
