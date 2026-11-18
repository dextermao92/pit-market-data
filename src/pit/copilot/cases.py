"""Eval cases for the Copilot: evals/cases/{real,simulated}/*.yaml.

Each case: id, simulated, today (the agent's "today"), question, expected.categories (any one counts as correct; the
first is the primary label), expected.runbooks (the ops docs that are a correct pointer), label.status (verified or
needs_review) and label.source (how the label was checked). Simulated cases also carry `sim`, the faults injected
into a copy of the data (pit.copilot.fixtures). A real case with `own_data: true` (a day after the frozen base, drafted
by pit.copilot.drafts) reads its own copy: the base plus production's data for the missing days.

needs_review cases are drafts: the eval leaves them out of the score (and out of the memory pool) until a person
checks the label and sets label.status to verified.

  uv run python -m pit.copilot.cases            validate every case and build the simulated cases' data dirs
"""

import sys
from datetime import date
from pathlib import Path

import yaml

from pit.copilot.categories import CATEGORIES
from pit.copilot.fixtures import BASE, FIXTURES, ROOT, build_case, build_real_case

CASES = ROOT / "evals" / "cases"
DOCS = {p.relative_to(ROOT / "ops").with_suffix("").as_posix() for p in (ROOT / "ops").rglob("*.md")}
KINDS = ("real", "simulated")  # the diagnosis eval
INJECTION = "injection"  # prompt-injection set: simulated cases with planted instructions, scored on blocking


def load_cases(root: Path = CASES, kinds=(*KINDS, INJECTION)) -> list[dict]:
    cases = []
    for kind in kinds:
        for f in sorted((root / kind).glob("*.yaml")):
            case = yaml.safe_load(f.read_text(encoding="utf-8"))
            case["today"] = str(case["today"])
            cases.append(case | {"_file": f.relative_to(root).as_posix(), "_kind": kind})
    return cases


def verified(case: dict) -> bool:
    return case.get("label", {}).get("status") == "verified"


def problems(case: dict, docs: set[str] = DOCS) -> list[str]:
    out, exp = [], case.get("expected", {})
    for key in ("id", "question", "today", "expected", "label"):
        if not case.get(key):
            out.append(f"missing {key}")
    if case.get("simulated") is not (case["_kind"] != "real"):
        out.append("simulated must be false exactly for cases under real/")
    if case["_kind"] != "real" and "sim" not in case:
        out.append("simulated case without sim")
    out += [f"unknown category {c!r}" for c in exp.get("categories") or ["<none>"] if c not in CATEGORIES]
    out += [f"unknown runbook {r!r}" for r in exp.get("runbooks") or ["<none>"] if r not in docs]
    if case.get("label", {}).get("status") not in ("verified", "needs_review"):
        out.append("label.status must be verified or needs_review")
    return out


def data_dir(case: dict, base: Path = BASE, fixtures: Path = FIXTURES) -> Path:
    """Where this case's tools read from: the frozen base, or the simulated case's own copy (built if missing, and
    rebuilt if its copy of the ops docs is older than ops/: an edited runbook must reach the eval, and recordings)."""
    if not case.get("simulated"):
        if not case.get("own_data"):
            return base
        root = fixtures / case["id"]
        return root if root.exists() else (build_real_case(case["id"], date.fromisoformat(case["today"]),
                                                           base=base, fixtures=fixtures) or base)
    root = fixtures / case["id"]
    copies = list((root / "ops").rglob("*.md")) if root.exists() else []
    stale = copies and max(p.stat().st_mtime for p in (ROOT / "ops").rglob("*.md")) > min(
        p.stat().st_mtime for p in copies)
    return root if root.exists() and not stale else build_case(case["id"], case["sim"], base, fixtures)


def main() -> int:
    cases, bad = load_cases(), 0
    ids = [c["id"] for c in cases]
    for c in cases:
        p = problems(c) + (["duplicate id"] if ids.count(c["id"]) > 1 else [])
        bad += bool(p)
        flag = "needs_review" if c["label"]["status"] == "needs_review" else ""
        print(f"{'BAD ' if p else 'ok  '}{c['_file']:<55} {c['expected']['categories'][0]:<24} {flag}"
              + (f"  {p}" if p else ""))
    real = sum(not c["simulated"] for c in cases)
    print(f"\n{len(cases)} cases: {real} real, {len(cases) - real} simulated; "
          f"{sum(c['label']['status'] == 'needs_review' for c in cases)} labels need review; {bad} invalid")
    for c in cases:
        if c["simulated"]:
            build_case(c["id"], c["sim"])
    print(f"built {len(cases) - real} simulated data dirs under {FIXTURES}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
