"""Pre-publication secret scan (docs/public-release-checklist.md): every blob reachable from any ref, every commit
message and every author / committer, against the values in .env and patterns for keys, webhooks, IBKR account ids,
emails, user paths, and the extra terms in .env's SCAN_EXTRA_TERMS (comma-separated, matched case-insensitively:
e.g. old usernames that are no longer in .env). Prints key names, locations and masked matches, never a value.

  uv run python scripts/secret_scan.py .
"""
import collections
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(sys.argv[1])
NOT_SECRET = {"SCAN_EXTRA_TERMS", "TRADING_MODE", "IB_PORT", "AIRFLOW_UID", "TIME_ZONE", "TWOFA_DEVICE", "SMTP_HOST", "SMTP_PORT"}


def git(*a, input=None):
    return subprocess.run(["git", "-C", str(REPO), *a], capture_output=True, input=input, check=True).stdout


def mask(s):
    return s if len(s) <= 4 else f"{s[:2]}{'*' * (len(s) - 4)}{s[-2:]}"


env = {}
for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
    k, sep, v = line.partition("=")
    v = v.strip().strip('"').strip("'")
    if sep and not k.strip().startswith("#") and k.strip() not in NOT_SECRET and len(v) >= 5:
        env[k.strip()] = v
    if k.strip() == "SCAN_EXTRA_TERMS":
        for n, term in enumerate(t.strip() for t in v.split(",")):
            if len(term) >= 3:
                env[f"SCAN_EXTRA_TERMS[{n}]"] = term

PATTERNS = {
    "ibkr_account": re.compile(r"\b(?:DU|DF|U)\d{6,9}\b"),
    "discord_webhook": re.compile(r"discord(?:app)?\.com/api/webhooks/\d+/[\w-]+"),
    "openai_style_key": re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"),
    "aws_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}"),
    "alpaca_key_id": re.compile(r"\b(?:PK|AK)[A-Z0-9]{16,20}\b"),
    "private_key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "password_assign": re.compile(r"(?i)(?:password|passwd|pwd|secret|api_key|token)\s*[:=]\s*['\"]?([^\s'\"#,)]{6,})"),
    "email": re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)*\.[a-z]{2,}"),
    "tailscale_ip": re.compile(r"\b100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}\b"),
    "windows_user_path": re.compile(r"(?i)C:[\\/]+Users[\\/]+[\w.-]+"),
    "mac_user_path": re.compile(r"/Users/[\w.-]+"),
    "phone": re.compile(r"(?<!\d)(?:\+?61|0)4\d{2}[ -]?\d{3}[ -]?\d{3}(?!\d)"),
}

hits = collections.defaultdict(lambda: collections.defaultdict(set))  # kind -> masked value -> {paths}
envhits = collections.defaultdict(set)

# blobs: object -> paths (from all refs)
objs = git("rev-list", "--all", "--objects").decode().splitlines()
blob_paths = collections.defaultdict(set)
ids = []
for line in objs:
    sha, _, path = line.partition(" ")
    if path:
        blob_paths[sha].add(path)
        ids.append(sha)
types = git("cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            input="\n".join(dict.fromkeys(ids)).encode()).decode().splitlines()
blobs = [t.split()[0] for t in types if t.split()[1] == "blob"]


def scan(text, where):
    lower = text.lower()
    for k, v in env.items():
        if (v.lower() in lower) if k.startswith("SCAN_EXTRA_TERMS") else (v in text):
            envhits[k].add(where)
    for kind, rx in PATTERNS.items():
        for m in rx.finditer(text):
            val = m.group(1) if rx.groups else m.group(0)
            hits[kind][mask(val) if kind not in ("email", "windows_user_path", "mac_user_path",
                                                 "tailscale_ip") else val].add(where)


data = git("cat-file", "--batch", input="\n".join(blobs).encode())
i = 0
for sha in blobs:
    header_end = data.index(b"\n", i)
    _, _, size = data[i:header_end].split()
    body = data[header_end + 1: header_end + 1 + int(size)]
    i = header_end + 1 + int(size) + 1
    text = body.decode("utf-8", errors="replace")
    for p in sorted(blob_paths[sha])[:1]:
        scan(text, p)

log = git("log", "--all", "--format=%H%x00%an%x00%ae%x00%cn%x00%ce%x00%B%x01").decode("utf-8", errors="replace")
people = collections.Counter()
for rec in log.split("\x01"):
    if not rec.strip():
        continue
    h, an, ae, cn, ce, body = rec.strip("\n").split("\x00", 5)
    people[(an, ae)] += 1
    people[(cn, ce)] += 0
    scan(body, f"commit message {h[:7]}")
tags = git("for-each-ref", "refs/tags", "--format=%(refname)").decode().split()

print(f"refs scanned: all ({len(git('rev-list', '--all').split())} commits), {len(blobs)} blobs, tags: {tags or 'none'}")
print("\n== .env values found in history (key names only):")
for k in env:
    print(f"  {k}: {sorted(envhits[k])[:6] if envhits[k] else 'not found'}")
print("\n== authors / committers:")
for (n, e), c in people.most_common():
    print(f"  {n} <{e}>  ({c} commits authored)")
for kind in PATTERNS:
    print(f"\n== {kind}: {len(hits[kind])} distinct")
    for v, where in sorted(hits[kind].items(), key=lambda x: -len(x[1]))[:25]:
        w = sorted(where)
        print(f"  {v!r}: {len(w)} places, e.g. {w[:4]}")
