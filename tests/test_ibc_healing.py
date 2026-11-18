import socket
import threading

from pit import ibc
from pit.health import gateway_with_healing


def test_command_round_trip(monkeypatch):
    srv = socket.create_server(("127.0.0.1", 0))
    got = []

    def serve():
        conn, _ = srv.accept()
        with conn:
            got.append(conn.recv(1024).decode())
            conn.sendall(b"OK\n")
            got.append(conn.recv(1024).decode())

    server = threading.Thread(target=serve, daemon=True)
    server.start()
    monkeypatch.setenv("IB_HOST", "127.0.0.1")
    monkeypatch.setenv("IBC_COMMAND_PORT", str(srv.getsockname()[1]))
    assert ibc.command("RECONNECTDATA") == "OK"
    server.join(timeout=5)
    assert got == ["RECONNECTDATA\n", "EXIT\n"]


def healing(results, failing_commands=(), in_window=True, raises=None):
    """Run gateway_with_healing with scripted check results; returns (level, fields, commands sent).
    raises: {command: exception} for IBC answering a command badly."""
    checks, sent = iter(results), []

    def send(cmd):
        sent.append(cmd)
        if cmd in failing_commands:
            raise ConnectionRefusedError("no command server")
        if cmd in (raises or {}):
            raise raises[cmd]
        return "OK"

    import pit.health
    original = pit.health.heal_window
    pit.health.heal_window = lambda now: in_window
    try:
        level, fields = gateway_with_healing({}, check=lambda cfg: next(checks), send_command=send, sleep=lambda s: None)
    finally:
        pit.health.heal_window = original
    return level, fields, sent


OK, BAD = ("ok", [("✅ Gateway", "已登录", False)]), ("error", [("❌ Gateway", "美股报价：无", False)])


def test_healthy_gateway_is_left_alone():
    assert healing([OK]) == ("ok", OK[1], [])


def test_reconnect_heals_and_is_reported_yellow():
    level, fields, sent = healing([BAD, OK])
    assert level == "warn" and sent == ["RECONNECTDATA"]
    assert fields[0][0] == "🔧 自动修复" and "✅ 已恢复" in fields[0][1] and fields[1] == OK[1][0]


def test_escalates_to_restart_then_gives_up_red():
    level, fields, sent = healing([BAD, BAD, BAD])
    assert level == "error" and sent == ["RECONNECTDATA", "RESTART"]
    assert "自动修复失败" in fields[0][1]


def test_unreachable_command_server_still_tries_next_step():
    level, fields, sent = healing([BAD, OK], failing_commands={"RECONNECTDATA"})
    assert level == "warn" and sent == ["RECONNECTDATA", "RESTART"] and "无法连接 IBC 命令服务器" in fields[0][1]


def test_denied_command_still_tries_next_step():
    level, fields, sent = healing([BAD, OK], raises={"RECONNECTDATA": ibc.Denied("hung up")})
    assert level == "warn" and sent == ["RECONNECTDATA", "RESTART"] and "IBC 拒绝了命令" in fields[0][1]


def test_hung_command_server_stops_escalating():
    # A RESTART would only queue behind the stuck RECONNECTDATA (2026-10-27): say so and ask for a manual restart.
    level, fields, sent = healing([BAD], raises={"RECONNECTDATA": ibc.Hung("no reply")})
    assert level == "error" and sent == ["RECONNECTDATA"]
    assert "命令服务器卡住" in fields[0][1] and "docker compose restart ib-gateway" in fields[0][1]


def test_stalled_contract_definitions_go_straight_to_restart():
    from pit.health import SECDEF_DOWN
    stalled = ("error", [("✅ Gateway", "已登录", False), (SECDEF_DOWN, "20s 内没有回应", False)])
    level, fields, sent = healing([stalled, OK])
    assert level == "warn" and sent == ["RESTART"]


def serve_once(monkeypatch, handle):
    srv = socket.create_server(("127.0.0.1", 0))
    threading.Thread(target=lambda: handle(srv.accept()[0]), daemon=True).start()
    monkeypatch.setenv("IB_HOST", "127.0.0.1")
    monkeypatch.setenv("IBC_COMMAND_PORT", str(srv.getsockname()[1]))
    return srv


def test_command_hung_up_on_is_denied(monkeypatch):
    import pytest
    serve_once(monkeypatch, lambda conn: (conn.recv(1024), conn.close()))  # IBC denying: reads, hangs up
    with pytest.raises(ibc.Denied, match="access denied"):
        ibc.command("RECONNECTDATA")


def test_command_never_answered_is_hung(monkeypatch):
    import pytest
    held = []
    serve_once(monkeypatch, held.append)  # takes the connection, never answers, never closes
    with pytest.raises(ibc.Hung, match="no reply"):
        ibc.command("RECONNECTDATA", reply_timeout=0.2)
    assert isinstance(ibc.Hung(), OSError) and isinstance(ibc.Denied(), OSError)  # old `except OSError` still catch


def test_check_gateway_reports_stalled_contract_definitions(monkeypatch):
    import pit.health as health
    seen = {}

    class Stalled:
        RequestTimeout = 300

        def qualifyContracts(self, *contracts):
            seen["timeout"] = self.RequestTimeout
            raise TimeoutError()

        def disconnect(self):
            seen["disconnected"] = True

    monkeypatch.setattr(health, "connect", lambda cfg, client_offset=0: (Stalled(), []))
    level, fields = health.check_gateway({})
    assert level == "error" and fields[1][0] == health.SECDEF_DOWN
    assert seen == {"timeout": health.SECDEF_TIMEOUT, "disconnected": True}


def test_check_gateway_fails_on_missing_subscription_even_with_farms_up(monkeypatch):
    from types import SimpleNamespace
    import pit.health as health

    class NotSubscribed:
        RequestTimeout = 300

        def qualifyContracts(self, *contracts):
            return list(contracts)

        def reqMktData(self, contract):
            return SimpleNamespace(contract=contract, bid=-1, last=-1, close=100.0)

        def disconnect(self):
            pass

    errors = [{"code": 2104, "msg": "Market data farm connection is OK:usfarm.nj"}, {"code": 10089, "msg": "needs subscription"}]
    monkeypatch.setattr(health, "connect", lambda cfg, client_offset=0: (NotSubscribed(), errors))
    monkeypatch.setattr(health, "wait_for_ticks", lambda *a: None)
    level, fields = health.check_gateway({})
    assert level == "error" and fields[2][0] == health.NO_DATA_FIELD and "10089" in fields[2][1]


def test_subscription_problem_is_not_healed():
    from pit.health import NO_DATA_FIELD
    missing = ("error", [("✅ Gateway", "已登录", False), (NO_DATA_FIELD, "错误码 [354]", False)])
    assert healing([missing]) == ("error", missing[1], [])


def test_no_healing_outside_the_window():
    level, fields, sent = healing([BAD], in_window=False)
    assert level == "error" and sent == []


def test_heal_window():
    from datetime import datetime, timezone
    from pit.health import heal_window
    assert heal_window(datetime(2026, 10, 19, 13, 2, tzinfo=timezone.utc))        # Mon 09:02 ET
    assert not heal_window(datetime(2026, 10, 18, 11, 0, tzinfo=timezone.utc))    # Sunday
    assert not heal_window(datetime(2026, 10, 20, 1, 0, tzinfo=timezone.utc))     # Mon 21:00 ET


def test_hung_request_is_a_failed_check_so_healing_runs(monkeypatch):
    import pit.health
    class HungIB:  # contract definitions answer, the next request hangs (a secdef hang has its own field)
        RequestTimeout = 0
        def qualifyContracts(self, *c):
            return list(c)
        def reqMktData(self, c):
            raise TimeoutError()
        def disconnect(self):
            self.closed = True
    ib = HungIB()
    monkeypatch.setattr(pit.health, "connect", lambda cfg, client_offset=0: (ib, []))
    level, fields = pit.health.check_gateway({})
    assert level == "error" and "请求没有响应" in fields[0][1] and ib.closed and ib.RequestTimeout == pit.health.SECDEF_TIMEOUT


def test_watchdog_heals_gateway_once_per_stale_episode(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    import pit.health
    import pit.notify
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(pit.notify, "send", lambda *a, **k: None)
    heals = []
    heal = lambda: heals.append(1) or ("warn", [("🔧 自动修复", "→ ✅ 已恢复", False)])
    at = lambda h, m: datetime(2026, 10, 26, h, m, tzinfo=timezone.utc)   # EDT: 13:45 UTC = 09:45 ET, no runs at all
    assert set(pit.health.watchdog(at(13, 46), heal=heal)) == {"option_snapshots", "index_snapshots"}
    assert pit.health.watchdog(at(13, 51), heal=heal) == []               # same episode: no new alert, no new restart
    assert heals == [1]
