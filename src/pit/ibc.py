"""Commands to IBC's command server inside the Gateway container (enabled in docker-compose, ControlFrom=airflow).

  RECONNECTDATA  reconnect all market-data farms (Ctrl-Alt-F in TWS): no restart, no 2FA
  RESTART        restart the Gateway through IBC's auto-restart path: no 2FA
"""

import os
import socket
import time


class Denied(ConnectionError):
    """IBC hung up without replying: it refuses commands from this host (ControlFrom)."""


class Hung(TimeoutError):
    """IBC took the command but never replied. Its command server handles one command at a time, so later commands
    (RESTART too) queue behind it: on 2026-10-27 a RECONNECTDATA stuck there and only a container restart helped."""


def command(cmd: str, timeout: float = 10, reply_timeout: float = 3) -> str:
    """Send one command, return IBC's reply (e.g. "OK"). Raises OSError if the server can't be reached,
    Denied / Hung (both OSErrors too) if it answers nothing."""
    host = os.environ.get("IB_HOST", "127.0.0.1")
    port = int(os.environ.get("IBC_COMMAND_PORT", "7462"))
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(f"{cmd}\n".encode())
        time.sleep(1)
        s.settimeout(reply_timeout)
        reply, hung = b"", False
        try:
            while chunk := s.recv(4096):
                reply += chunk
                if b"\n" in chunk:
                    break
        except TimeoutError:  # connection kept open, nothing said
            hung = True
        except ConnectionError:  # IBC may hang up without a word when it denies access (reset / aborted)
            pass
        try:
            s.sendall(b"EXIT\n")
        except OSError:
            pass  # the server may already have closed (e.g. after denying access)
    reply = reply.decode(errors="replace").strip()
    if not reply and hung:
        raise Hung(f"IBC took {cmd} but sent no reply within {reply_timeout + 1:.0f}s (command server stuck? "
                   "restart the Gateway container)")
    if not reply:
        raise Denied(f"IBC hung up without replying to {cmd} (access denied? check ControlFrom)")
    return reply
