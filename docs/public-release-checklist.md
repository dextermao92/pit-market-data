# Public release checklist

**Status:** released. All six decisions below are done; the rebuilt history went to a new, public repository (the old
one was deleted). This page lists what was checked and what was decided. It records no secret or personal value, only
the kind of thing found and where.

**Scanned:** 2026-11-18, first at `main` = `54eb769` (109 commits, 789 blobs, refs `main`, `copilot-evidence-check`
and their `origin/` copies), then again after the rebuild (`main` only, 111 commits, 795 blobs). No tags or stashes.

- After the rebuild every commit has the noreply author, and every `.env` value and `SCAN_EXTRA_TERMS` entry is
  "not found". Collector usernames are no longer hard-coded in the scanner; it reads them from `.env`.
- Tool: [`scripts/secret_scan.py`](../scripts/secret_scan.py). It prints key names, locations and masked values,
  never a value. Rerun it right before publishing:

  ```bash
  uv run python scripts/secret_scan.py .
  ```

## Checked, clean

| Check | How | Result |
|---|---|---|
| `.env`, `data/`, `evals/fixtures/` never committed | every path in every commit on all refs (292 paths) | None of them, and no `*.duckdb`, `*.parquet`, keys or certificates; only `.env.example` |
| `.env.example` holds no values | read it | Every secret is empty; only `TRADING_MODE` and `IB_PORT` have defaults |
| `.env` values (IBKR username and password, VNC password, Alpaca key and secret, Discord webhook, DeepSeek key) | each value searched for in every file version and commit message | Not found |
| API keys and tokens (`sk-…`, `AKIA…`, `ghp_…`, Alpaca `PK…`/`AK…`, private keys) | patterns over every file version and commit message | None |
| Discord webhooks | pattern | One: a placeholder in `tests/test_copilot_llm.py` (`webhooks/123/abc-DEF`) |
| IBKR account ids (`U` / `DU` + 6–9 digits) | pattern | Only test placeholders (`U1234567`, `DU7654321`); the real account id appears nowhere |
| Collector IBKR usernames (the current one and the old one) | searched in file contents and commit messages | Not found |
| Email addresses in files and messages | pattern | Only `noreply@anthropic.com` (commit trailers) and `me@example.com` (test) |
| Local paths, computer name, Tailscale | `C:\Users\…`, `/Users/…`, the host name, `ts.net`, `100.64/10` addresses | None |
| `config/` | read `alpaca_bars.toml`, `ibkr.toml`, `llm.toml` | Symbols, limits and prices only: no account, user or key |
| CI | `.github/workflows/ci.yml` | Uses no secrets; replays recordings with no API key |
| Eval recordings | `tests/test_copilot_evalset.py` scans them for account ids, emails, webhooks and keys on every test run | Clean |

**False positives the patterns also hit:**

- password-like assignments: compose's `${VAR}` references, code such as `payload = …`;
- a "phone number" that is part of a hash in `uv.lock`;
- `sk-` inside words like `disk-full` and `task-`.

## Decisions (2026-11-18)

| Item | Decision |
|---|---|
| 1. Personal email in 6 commits | History rebuilt: every commit authored with the GitHub noreply address |
| 2. Overwritten commits still on GitHub | Old repository deleted; the rebuilt history went to a new one under the same name |
| 3. AEDT time-zone hint | Accepted |
| 4. `databricks/` not ignored | Added to `.gitignore` |
| 5. No LICENSE | MIT ([`LICENSE`](../LICENSE)) |
| 6. Default Postgres password | Accepted (local only) |

## Findings as scanned (before the decisions)

1. **A personal email address is in 6 commits' author and committer fields.** The commits are `3851be2`,
   `eb48e1c`, `ebbd0fe`, `c294c87`, `701738c` and `b7fc328`, made on the Mac between 2026-11-10 and 11-18.
   - The other 103 commits use the GitHub noreply address. The 11-13 history rewrite switched to it, but the Mac's
     clone falls back to the global git email.
   - Options:
     - rewrite those commits' author (`git filter-repo --mailmap`) and force-push again;
     - accept the address as public;
     - publish a fresh repo (item 2).
   - Either way, set the repo-local `user.email` to the noreply address on the Mac. This Windows clone already
     has it.
2. **The commits overwritten on 2026-11-13 are still on GitHub.**
   - Before the rewrite, history held the old IBKR username (in a runbook and a commit message) and the personal
     email.
   - GitHub keeps serving overwritten commits by SHA until the repository is deleted or GitHub Support purges
     them. The Actions run history also links to them. Once the repo is public, anyone browsing the old CI runs
     can open them.
   - Options:
     - **publish from a new repository** that gets only the clean history: the simplest complete fix;
     - delete the old workflow runs and ask GitHub Support to remove the cached commits;
     - accept it.
3. **A rough location shows through a time zone.** `ops/incidents/2026-10-26.md` and many eval recordings and
   results say AEDT (and "overnight in their time zone"). It reveals a region, not an address.
   - Low risk.
   - Options: keep it, or reword it to "the operator's night" and re-record. Re-recording costs about $0.2, and
     the old recordings stay in history.
4. **`databricks/` is untracked but not ignored**, so a `git add .` would publish personal study notes.
   - Add it to `.git/info/exclude`. That's local, and unlike `.gitignore` it doesn't publish the folder name.
5. **There's no LICENSE.** Public code without one is all rights reserved: readable, but nobody may reuse it.
   - Options: add one (MIT is common for portfolio projects) or leave it on purpose.
6. **Postgres runs with the default password `airflow`** in `docker-compose.yml`.
   - It's Airflow metadata only, on the compose network, with no host port. That's acceptable; just know it's
     visible.

## Before flipping to public

- [x] Decide items 1–6 above.
- [x] Rerun `scripts/secret_scan.py`; `.env` values found should still be "not found" for every key.
- [x] Check `git status` for untracked personal files.
- [x] Make it public by hand, on GitHub. Nothing in this repo or its automation changes visibility.
