import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from pit.copilot.llm import LLM, BudgetExceeded, cost_usd, is_peak, load_config, report
from pit.copilot.redact import redact

CFG = load_config()


def test_redact_masks_secrets_accounts_webhooks_and_emails():
    env = {"TWS_PASSWORD": "hunter2-secret", "APCA_API_KEY_ID": "PKTEST1234567890", "IB_PORT": "4003", "SHORT_KEY": "abc"}
    text = ("login with hunter2-secret, key PKTEST1234567890 on port 4003; account U1234567 and paper DU7654321; "
            "post to https://discord.com/api/webhooks/123/abc-DEF; mail me@example.com")
    out, n = redact(text, env)
    assert out == ("login with [TWS_PASSWORD], key [APCA_API_KEY_ID] on port 4003; account [IBKR_ACCOUNT] and paper "
                   "[IBKR_ACCOUNT]; post to [WEBHOOK]; mail [EMAIL]")
    assert n == 6
    assert redact("SPX 7623.68, run U2 of 78", env) == ("SPX 7623.68, run U2 of 78", 0)  # ordinary text untouched


@pytest.mark.parametrize("at, peak", [
    (datetime(2026, 11, 10, 2, 30, tzinfo=timezone.utc), True),    # Tue 02:30 UTC
    (datetime(2026, 11, 10, 4, 0, tzinfo=timezone.utc), False),    # window ends at 04:00
    (datetime(2026, 11, 10, 9, 59, tzinfo=timezone.utc), True),
    (datetime(2026, 11, 14, 2, 30, tzinfo=timezone.utc), False),   # Saturday
])
def test_peak_hours(at, peak):
    assert is_peak(at, CFG) is peak


def test_cost_splits_cache_hits_and_doubles_at_peak():
    usage = {"cache_hit_tokens": 1_000_000, "cache_miss_tokens": 1_000_000, "completion_tokens": 1_000_000}
    off = datetime(2026, 11, 14, 2, 30, tzinfo=timezone.utc)
    assert cost_usd(usage, "deepseek-flash", off, CFG) == pytest.approx(0.003 + 0.15 + 0.60)
    assert cost_usd(usage, "deepseek-flash", datetime(2026, 11, 10, 2, 30, tzinfo=timezone.utc), CFG) == \
        pytest.approx(2 * (0.003 + 0.15 + 0.60))


class FakeClient:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, **kw):
        self.sent.append(kw)
        if self.fail:
            raise TimeoutError("read timeout")
        usage = SimpleNamespace(prompt_tokens=1200, completion_tokens=300, prompt_cache_hit_tokens=1000,
                                completion_tokens_details=SimpleNamespace(reasoning_tokens=0))
        msg = SimpleNamespace(content="The Gateway API froze at 09:16 ET.", tool_calls=None)
        return SimpleNamespace(id="req-1", usage=usage, choices=[SimpleNamespace(message=msg, finish_reason="stop")])


def test_call_is_redacted_logged_and_costed(tmp_path, monkeypatch):
    monkeypatch.setenv("TWS_PASSWORD", "hunter2-secret")
    fake = FakeClient()
    llm = LLM(cfg=CFG, client=fake, log_dir=tmp_path)
    llm.chat([{"role": "user", "content": "why did U1234567 fail? password hunter2-secret"}], purpose="test")

    sent = fake.sent[0]
    assert sent["messages"][0]["content"] == "why did [IBKR_ACCOUNT] fail? password [TWS_PASSWORD]"
    assert sent["model"] == "deepseek-flash" and sent["extra_body"] == {"thinking": {"type": "disabled"}}
    row = json.loads(next(tmp_path.glob("*.jsonl")).read_text())
    assert (row["status"], row["redactions"], row["cache_hit_tokens"], row["cache_miss_tokens"]) == ("ok", 2, 1000, 200)
    assert row["cost_usd"] > 0 and row["latency_s"] >= 0 and "content" not in json.dumps(row)  # no text in the log


def test_temperature_comes_from_config_and_is_logged(tmp_path):
    fake = FakeClient()
    LLM(cfg=CFG | {"llm": CFG["llm"] | {"temperature": 0}}, client=fake, log_dir=tmp_path).chat(
        [{"role": "user", "content": "hi"}], purpose="test")
    without = {k: v for k, v in CFG["llm"].items() if k != "temperature"}
    LLM(cfg=CFG | {"llm": without}, client=fake, log_dir=tmp_path).chat([{"role": "user", "content": "hi"}],
                                                                        purpose="test")
    assert fake.sent[0]["temperature"] == 0 and "temperature" not in fake.sent[1]  # omitted = API default
    rows = [json.loads(x) for x in next(tmp_path.glob("*.jsonl")).read_text().splitlines()]
    assert [r["temperature"] for r in rows] == [0, None]


def test_failed_call_is_logged_and_raised(tmp_path):
    llm = LLM(cfg=CFG, client=FakeClient(fail=True), log_dir=tmp_path)
    with pytest.raises(TimeoutError):
        llm.chat([{"role": "user", "content": "hi"}], purpose="test")
    row = json.loads(next(tmp_path.glob("*.jsonl")).read_text())
    assert row["status"] == "error" and "read timeout" in row["error"]


def test_daily_budget_stops_calls(tmp_path):
    cfg = CFG | {"llm": CFG["llm"] | {"daily_budget_usd": 1e-9}}
    fake = FakeClient()
    llm = LLM(cfg=cfg, client=fake, log_dir=tmp_path)
    llm.chat([{"role": "user", "content": "hi"}], purpose="test")  # nothing spent yet: allowed
    with pytest.raises(BudgetExceeded):
        llm.chat([{"role": "user", "content": "hi"}], purpose="test")
    assert len(fake.sent) == 1
    r = report(tmp_path).to_dicts()[0]
    assert (r["calls"], r["errors"], r["prompt_tokens"]) == (1, 0, 1200)
