"""Mask secrets and identifiers before text leaves the machine (prompts, tool results sent to the LLM).

Gateway and task logs carry the IBKR account number (positions are logged on connect), and a pasted .env or
config could carry keys and webhooks. Masked: the values of secret-looking env vars, IBKR account ids, Discord
webhook URLs, email addresses.
"""

import os
import re

SECRET_ENV = re.compile(r"KEY|SECRET|PASSWORD|TOKEN|WEBHOOK|USERID")
PATTERNS = [
    (re.compile(r"https://(?:\w+\.)?discord(?:app)?\.com/api/webhooks/[\w/-]+"), "[WEBHOOK]"),
    (re.compile(r"\b(?:DU|U)\d{6,9}\b"), "[IBKR_ACCOUNT]"),  # live U…, paper DU…
    (re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b"), "[EMAIL]"),
]


def redact(text: str, env: dict | None = None) -> tuple[str, int]:
    """(masked text, number of replacements)."""
    env = os.environ if env is None else env
    n = 0
    # longest first, so a value containing another is masked whole
    for name, value in sorted(((k, v) for k, v in env.items() if SECRET_ENV.search(k) and len(v) >= 6),
                              key=lambda kv: -len(kv[1])):
        n += text.count(value)
        text = text.replace(value, f"[{name}]")
    for pattern, label in PATTERNS:
        text, k = pattern.subn(label, text)
        n += k
    return text, n
