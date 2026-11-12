"""Tools with side effects for the Copilot. Each one runs only after a person approves it in the terminal.

Kept out of the MCP server (pit.copilot.mcp_server stays read-only): only the agent loop offers these tools, and
when the model calls one the loop, not the model, asks the approver. Nothing the model writes and nothing a tool
returns can approve: the tool takes no approval argument, and the terminal approver reads only from an interactive
terminal and wants the command name typed out. Approval is per call and doesn't carry over.

Never executed, whatever the approver says, when CI is set or PIT_COPILOT_NO_ACTIONS=1 (evals and CI).
"""

import os
import sys
from collections.abc import Callable
from dataclasses import dataclass

from pit import ibc


@dataclass(frozen=True)
class Action:
    name: str
    command: str  # the IBC command sent once approved
    description: str  # for the model
    effect: str  # for the person approving
    warning: str


RECONNECT = Action(
    name="reconnect_market_data",
    command="RECONNECTDATA",
    description="PROPOSE reconnecting the IB Gateway's market-data farms (IBC RECONNECTDATA). Has side effects: "
                "it runs only if the operator approves it in the terminal, and is usually denied. Propose it only "
                "when the runs show farms disconnected (codes 1100/2103/2105/2157) and the runbook says so.",
    effect="Sends RECONNECTDATA to IBC's command server: the Gateway reconnects all market-data farms. "
           "No restart, no 2FA. Running snapshots may fail while farms reconnect (about 20 s).",
    warning="Don't send it into a contract-definition (secdef) stall: on 2026-10-27 that hung IBC's command server "
            "and only `docker compose restart ib-gateway` helped (ops/runbooks/gateway.md, section B).",
)
ACTIONS = {a.name: a for a in (RECONNECT,)}


def schema(a: Action) -> dict:
    return {"type": "function", "function": {"name": a.name, "description": a.description, "parameters": {
        "type": "object", "required": ["reason"],
        "properties": {"reason": {"type": "string", "description": "Why, citing the runs and the runbook."}}}}}


Approver = Callable[[Action, str], bool]  # (action, the model's reason) -> approved?


def deny_all(action: Action, reason: str) -> bool:
    return False


def terminal_approver(ask: Callable[[str], str] = input, isatty: Callable[[], bool] = lambda: sys.stdin.isatty(),
                      out=None) -> Approver:
    """Asks on the terminal; approved only if the person types the IBC command name exactly. Piped or redirected
    stdin (a script, CI, another program) is always a denial."""
    def approve(action: Action, reason: str) -> bool:
        nonlocal out
        out = out or sys.stderr
        if not isatty():
            print(f"[approval] {action.name}: denied, no interactive terminal", file=out)
            return False
        print(f"\n=== Approval needed: {action.name} ===\n{action.effect}\nCaution: {action.warning}\n"
              f"Reason given by the model (unverified):\n  {reason!r}", file=out)
        try:
            answer = ask(f"Type {action.command} to run it, anything else to deny: ")
        except EOFError:
            return False
        return answer.strip() == action.command
    return approve


def actions_disabled() -> bool:
    return bool(os.environ.get("CI")) or os.environ.get("PIT_COPILOT_NO_ACTIONS") == "1"


def execute(action: Action, send_command: Callable[[str], str] | None = None) -> str:
    """Run an approved action; returns what to tell the model."""
    if actions_disabled():
        return f"Not run: actions are disabled here (CI / PIT_COPILOT_NO_ACTIONS). {action.command} was not sent."
    try:
        reply = (send_command or ibc.command)(action.command)
    except OSError as e:  # ibc.Hung and ibc.Denied are OSErrors too
        return f"Approved, but {action.command} failed: {e!r}"
    return f"Approved by the operator and run: IBC replied {reply!r}."
