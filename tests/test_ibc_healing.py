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


def healing(results, failing_commands=(), in_window=True):
    """Run gateway_with_healing with scripted check results; returns (level, fields, commands sent)."""
    checks, sent = iter(results), []

    def send(cmd):
        sent.append(cmd)
        if cmd in failing_commands:
            raise ConnectionRefusedError("no command server")
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


def test_command_without_reply_raises(monkeypatch):
    import pytest
    srv = socket.create_server(("127.0.0.1", 0))
    threading.Thread(target=lambda: srv.accept()[0].close(), daemon=True).start()  # IBC denying: hangs up
    monkeypatch.setenv("IB_HOST", "127.0.0.1")
    monkeypatch.setenv("IBC_COMMAND_PORT", str(srv.getsockname()[1]))
    with pytest.raises(ConnectionError, match="no reply"):
        ibc.command("RECONNECTDATA")


def test_no_healing_outside_the_window():
    level, fields, sent = healing([BAD], in_window=False)
    assert level == "error" and sent == []


def test_heal_window():
    from datetime import datetime, timezone
    from pit.health import heal_window
    assert heal_window(datetime(2026, 10, 19, 13, 2, tzinfo=timezone.utc))        # Mon 09:02 ET
    assert not heal_window(datetime(2026, 10, 18, 11, 0, tzinfo=timezone.utc))    # Sunday
    assert not heal_window(datetime(2026, 10, 20, 1, 0, tzinfo=timezone.utc))     # Mon 21:00 ET
