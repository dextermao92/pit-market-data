"""POST /ask: the agent loop behind a bearer token, with a scripted model (no API key, runs in CI)."""

import pytest
from fastapi.testclient import TestClient

from pit.copilot.api import create_app
from pit.copilot.llm import LLM, load_config
from test_copilot_agent import CITED, GOOD, Scripted, call, reply
from test_mcp_server import write_runs

TOKEN = "t" * 32
Q = {"question": "Why no option snapshots on 2026-10-26 morning?"}
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def api(tmp_path, monkeypatch):
    """(make a client for these scripted replies, tmp data dir). Env changes are undone after the test."""
    write_runs(tmp_path)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PIT_COPILOT_NO_ACTIONS", "0")  # create_app sets it to 1; monkeypatch restores it after
    monkeypatch.setenv("PIT_AGENT_QUIET", "1")

    def make(*replies, cfg=None, token=TOKEN):
        llm = LLM(cfg=cfg or load_config(), client=Scripted(*replies), log_dir=tmp_path / "llm_calls")
        return TestClient(create_app(lambda: llm, data_dir=tmp_path, token=token))

    return make, tmp_path


def test_ask_returns_diagnosis_tool_calls_tokens_and_cost(api):
    make, _ = api
    client = make(reply(call("get_runs", {"day": "2026-10-26", "status": "failed"})),
                  reply(call("submit_diagnosis", CITED, "c2")))
    r = client.post("/ask", json=Q, headers=AUTH)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "ok" and out["diagnosis"] == CITED and out["steps"] == 2
    assert [(t["step"], t["tool"]) for t in out["tool_calls"]] == [(1, "get_runs"), (2, "submit_diagnosis")]
    assert out["tool_calls"][0]["args"] == {"day": "2026-10-26", "status": "failed"}
    assert out["llm_calls"] == 2 and out["tokens"]["prompt"] == 200 and out["tokens"]["completion"] == 40
    assert out["cost_usd"] > 0 and out["wall_s"] >= out["llm_latency_s"] >= 0
    assert out["actions_requested"] == [] and out["rejections"] == []


def test_ask_needs_the_token_and_is_off_without_one(api):
    make, _ = api
    client = make()
    assert client.post("/ask", json=Q).status_code == 401
    assert client.post("/ask", json=Q, headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.post("/ask", json=Q, headers={"Authorization": f"Basic {TOKEN}"}).status_code == 401
    assert client.get("/health").json() == {"ok": True}  # no token, no data
    for weak in ("", "short"):  # no token configured (or a trivial one): /ask refuses to run at all
        assert make(token=weak).post("/ask", json=Q, headers={"Authorization": f"Bearer {weak}"}).status_code == 503


def test_risky_tool_is_denied_and_never_sent(api, monkeypatch):
    import os

    from pit import ibc
    sent = []
    monkeypatch.setattr(ibc, "command", lambda *a, **k: sent.append(a) or "OK")
    make, _ = api
    client = make(reply(call("reconnect_market_data", {"reason": "farms down"})),
                  reply(call("submit_diagnosis", GOOD, "c2")))
    out = client.post("/ask", json=Q, headers=AUTH).json()
    assert out["actions_requested"] == [{"tool": "reconnect_market_data", "reason": "farms down", "approved": False}]
    assert sent == [] and os.environ["PIT_COPILOT_NO_ACTIONS"] == "1"
    assert out["status"] == "ok"


def test_budget_spent_is_429_and_bad_input_is_422(api):
    make, _ = api
    cfg = load_config()
    cfg["llm"]["daily_budget_usd"] = 0
    r = make(reply(call("submit_diagnosis", GOOD)), cfg=cfg).post("/ask", json=Q, headers=AUTH)
    assert r.status_code == 429 and "budget" in r.json()["detail"]
    assert make().post("/ask", json={"question": "x"}, headers=AUTH).status_code == 422
    assert make().post("/ask", json={}, headers=AUTH).status_code == 422
