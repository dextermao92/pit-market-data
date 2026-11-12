import json
from datetime import datetime, timedelta, timezone

from pit import host_watch as hw

T0 = datetime(2026, 11, 12, 14, 0, tzinfo=timezone.utc)
DOWN = {"docker": hw.docker_down(False, False, T0 - timedelta(hours=5))}


def run_checks(seq, window=True):
    """Feed problem sets 5 minutes apart; return (final state, titles sent per check)."""
    state, sent = {}, []
    for i, problems in enumerate(seq):
        state, msgs = hw.step(state, problems, T0 + timedelta(minutes=5 * i), window)
        sent.append([m[0] for m in msgs])
    return state, sent


def test_alerts_after_two_checks_then_recovers_once():
    state, sent = run_checks([DOWN, DOWN, DOWN, {}, {}])
    assert sent[0] == [] and sent[2] == [] and sent[4] == []
    assert sent[1] == [DOWN["docker"]["title"]]
    assert sent[3] == [f"✅ 已恢复：{DOWN['docker']['title']}"]
    assert state == {}


def test_blip_shorter_than_confirm_is_silent():
    _, sent = run_checks([DOWN, {}, {}])
    assert sent == [[], [], []]


def test_reminds_hourly_only_inside_the_window():
    _, sent = run_checks([DOWN] * 15)  # 70 minutes
    assert [i for i, s in enumerate(sent) if s] == [1, 13]
    assert "仍未恢复，已 65 分钟" in sent[13][0]  # first seen at check 0, alerted at 1, reminded 60 min later
    _, sent = run_checks([DOWN] * 15, window=False)
    assert [i for i, s in enumerate(sent) if s] == [1]


def test_reboot_pending_alerts_once_without_recovery():
    p ={"reboot": {"title": "🔄 reboot", "detail": "", "level": "warn", "remind": False, "recover": False}}
    _, sent = run_checks([p] * 20 + [{}])
    assert sum(map(len, sent)) == 1 and sent[1] == ["🔄 reboot"]


def test_docker_down_explains_the_likely_cause():
    assert "还没有人登录" in hw.docker_down(False, False, T0)["detail"]
    assert "docker desktop start" in hw.docker_down(True, False, T0)["detail"]
    assert "docker desktop restart" in hw.docker_down(True, True, T0)["detail"]


def test_service_problems_from_compose_ps():
    rows = [{"Service": "airflow", "State": "running", "Health": "", "Status": "Up 2 hours"},
            {"Service": "postgres", "State": "running", "Health": "unhealthy", "Status": "Up 2 hours (unhealthy)"},
            {"Service": "ib-gateway", "State": "exited", "Health": "", "Status": "Exited (1) 3 minutes ago"}]
    lines = "\n".join(json.dumps(r) for r in rows)
    assert sorted(hw.service_problems(lines)) == ["service_ib-gateway", "service_postgres"]
    assert "2FA" in hw.service_problems(lines)["service_ib-gateway"]["detail"]
    assert sorted(hw.service_problems(json.dumps(rows[:1]))) == ["service_ib-gateway", "service_postgres"]  # array form, missing
    ok = [dict(r, State="running", Health="healthy" if r["Service"] == "postgres" else "") for r in rows]
    assert hw.service_problems("\n".join(json.dumps(r) for r in ok)) == {}


def test_load_env_takes_only_notification_keys(tmp_path, monkeypatch):
    for k in ("DISCORD_WEBHOOK_URL", "TWS_PASSWORD", "ALERT_EMAIL_TO"):
        monkeypatch.delenv(k, raising=False)
    env = tmp_path / ".env"
    env.write_text('TWS_PASSWORD=secret\nDISCORD_WEBHOOK_URL="https://discord.example/hook"\nALERT_EMAIL_TO=\n')
    hw.load_env(env)
    import os
    assert os.environ["DISCORD_WEBHOOK_URL"] == "https://discord.example/hook"
    assert "TWS_PASSWORD" not in os.environ and "ALERT_EMAIL_TO" not in os.environ
    monkeypatch.delenv("DISCORD_WEBHOOK_URL")  # load_env set it outside monkeypatch


def test_window_is_session_days_0800_to_close_plus_30():
    assert hw.in_window(datetime(2026, 11, 12, 13, 0, tzinfo=timezone.utc))       # Thu 08:00 ET
    assert not hw.in_window(datetime(2026, 11, 12, 12, 55, tzinfo=timezone.utc))  # 07:55 ET
    assert hw.in_window(datetime(2026, 11, 12, 21, 30, tzinfo=timezone.utc))      # 16:30 ET
    assert not hw.in_window(datetime(2026, 11, 12, 21, 35, tzinfo=timezone.utc))
    assert not hw.in_window(datetime(2026, 11, 14, 15, 0, tzinfo=timezone.utc))   # Saturday
    assert not hw.in_window(datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc))   # Thanksgiving
