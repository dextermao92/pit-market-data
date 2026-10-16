"""Commands to IBC's command server inside the Gateway container (enabled in docker-compose, ControlFrom=airflow).

  RECONNECTDATA  reconnect all market-data farms (Ctrl-Alt-F in TWS): no restart, no 2FA
  RESTART        restart the Gateway through IBC's auto-restart path: no 2FA
"""

import os
import socket
import time


def command(cmd: str, timeout: float = 10) -> str:
    """Send one command, return IBC's reply (e.g. "OK"). Raises OSError if the server can't be reached."""
    host = os.environ.get("IB_HOST", "127.0.0.1")
    port = int(os.environ.get("IBC_COMMAND_PORT", "7462"))
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(f"{cmd}\n".encode())
        time.sleep(1)
        s.settimeout(3)
        reply = b""
        try:
            while chunk := s.recv(4096):
                reply += chunk
                if b"\n" in chunk:
                    break
        except TimeoutError:
            pass
        s.sendall(b"EXIT\n")
    return reply.decode(errors="replace").strip()
