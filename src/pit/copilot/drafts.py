"""Turn diagnoses a person confirmed (the Copilot's memory, data/copilot_memory/) into draft eval cases.

Each new note becomes evals/cases/real/rNN-<day>-<category>.yaml with the note's question, its day, "today" (the
day it was confirmed), expected.categories / runbooks from the note, and label.status `needs_review`: the eval leaves
it out of the score until a person checks the label (and adds any other acceptable category or runbook) and sets it
to `verified`. label.source says which note it came from.

A draft about a day the frozen base (evals/fixtures/base) doesn't have also gets its own data copy: the base plus
production's runs and bars for the missing days (pit.copilot.fixtures.build_real_case). The base and data/ are only
read.

Skipped, with the reason printed: a note already drafted, a note whose (day, category) an existing case already
covers, two notes in one batch with the same (day, category), and a note with an unknown category or runbook.

  uv run python -m pit.copilot.drafts              write drafts (and data copies)
  uv run python -m pit.copilot.drafts --dry-run    print what it would write
"""

import argparse
import re
import sys
from datetime import date
from pathlib import Path

import yaml

from pit.copilot.cases import CASES, load_cases, problems
from pit.copilot.categories import CATEGORIES
from pit.copilot.fixtures import BASE, FIXTURES, ROOT, build_real_case
from pit.copilot.memory import load_notes, memory_dir
from pit.copilot.redact import redact

DAY = re.compile(r"\b(20\d\d-\d\d-\d\d)\b")
REAL_ID = re.compile(r"^r(\d+)-")


def note_day(note: dict) -> tuple[str, str]:
    """(day, where it came from): the first date in the question, else the day it was confirmed."""
    if m := DAY.search(note.get("question") or ""):
        return m.group(1), "the question"
    return str(note.get("confirmed_on")), "confirmed_on (the question has no date)"


def case_day(case: dict) -> str:
    if m := DAY.search(case.get("question") or "") or DAY.search(case.get("id") or ""):
        return m.group(1)
    return str(case.get("today"))


def draft(note: dict, number: int) -> dict:
    day, where = note_day(note)
    today = max(day, str(note.get("confirmed_on") or day))
    category, runbook = note.get("category"), str(note.get("runbook") or "").removesuffix(".md")
    evidence = "; ".join(note.get("evidence") or [])
    source = (f"Draft from memory note {note['_id']}, confirmed {note.get('confirmed_on')} by "
              f"{note.get('confirmed_by')}. Day from {where}. Confirmed root cause: {str(note.get('root_cause', '')).strip().rstrip('.')}."
              + (f" Evidence: {evidence}" if evidence else "")
              + " TO REVIEW: check the label against the runs and logs, add any other acceptable category or runbook,"
                " then set status to verified.")
    case = {"id": f"r{number:02d}-{day}-{str(category).replace('_', '-')}", "simulated": False, "today": today,
            "question": redact(note.get("question") or "")[0],
            "expected": {"categories": [category], "runbooks": [runbook]},
            "label": {"status": "needs_review", "note": note["_id"], "source": " ".join(redact(source)[0].split())}}
    return case


def plan(notes: list[dict], cases: list[dict]) -> tuple[list[dict], list[str]]:
    """(drafts to write, skip messages). Pure: no files touched."""
    drafted = {c["label"].get("note") for c in cases}
    covered = {(case_day(c), cat): c["id"] for c in cases for cat in c["expected"]["categories"]}
    number = max((int(m.group(1)) for c in cases if (m := REAL_ID.match(c["id"]))), default=0)
    out, skipped = [], []
    for n in notes:
        day, _ = note_day(n)
        key = (day, n.get("category"))
        if n["_id"] in drafted:
            skipped.append(f"{n['_id']}: already drafted")
        elif key in covered:
            skipped.append(f"{n['_id']}: {day} {n.get('category')} is already case {covered[key]}")
        elif n.get("category") not in CATEGORIES:
            skipped.append(f"{n['_id']}: unknown category {n.get('category')!r}")
        else:
            number += 1
            case = draft(n, number)
            bad = problems(case | {"_kind": "real"})
            if bad:
                number -= 1
                skipped.append(f"{n['_id']}: {'; '.join(bad)}")
                continue
            covered[key] = case["id"]
            out.append(case)
    return out, skipped


def write(case: dict, cases_dir: Path = CASES, src: Path = ROOT / "data", base: Path = BASE,
          fixtures: Path = FIXTURES) -> tuple[Path, Path | None]:
    """Write one draft, after its data copy if the base doesn't cover its days. Never replaces a file."""
    path = cases_dir / "real" / f"{case['id']}.yaml"
    if path.exists():
        raise FileExistsError(path)
    copy = build_real_case(case["id"], date.fromisoformat(case["today"]), src, base, fixtures)
    if copy:  # own_data right after simulated, where a reader looks for where the data comes from
        rest = {k: v for k, v in case.items() if k not in ("id", "simulated")}
        case = {"id": case["id"], "simulated": False, "own_data": True, **rest}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(case, sort_keys=False, allow_unicode=True, width=116, default_flow_style=None),
                    encoding="utf-8")
    return path, copy


def run(notes: list[dict], cases_dir: Path = CASES, src: Path = ROOT / "data", base: Path = BASE,
        fixtures: Path = FIXTURES, dry_run: bool = False) -> list[dict]:
    drafts, skipped = plan(notes, load_cases(cases_dir))
    for s in skipped:
        print(f"skip  {s}")
    for case in drafts:
        if dry_run:
            print(f"would write  real/{case['id']}.yaml  ({case['expected']['categories'][0]}, today {case['today']})")
            continue
        path, copy = write(case, cases_dir, src, base, fixtures)
        print(f"wrote  real/{path.name}" + (f"  (data copy: {copy})" if copy else "  (data: the frozen base)"))
    print(f"{len(notes)} notes: {len(drafts)} {'to draft' if dry_run else 'drafted'} (needs_review), "
          f"{len(skipped)} skipped")
    return drafts


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m pit.copilot.drafts", description=__doc__.split("\n")[0])
    p.add_argument("--memory-dir", type=Path, default=None, help="default: data/copilot_memory (PIT_MEMORY_DIR)")
    p.add_argument("--src", type=Path, default=ROOT / "data", help="production data, read for the data copies")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    run(load_notes(a.memory_dir or memory_dir()), src=a.src, dry_run=a.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
