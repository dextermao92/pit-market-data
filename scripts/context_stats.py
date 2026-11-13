"""Prompt-token statistics of an eval run: how big the context gets per diagnosis and how it grows step by step.

Reads an eval results file (evals/results/*.json) and the LLM call log (data/llm_calls/*.jsonl), matched by each
case's run_id. Replayed runs make no logged calls, so use it on live runs.

    uv run python scripts/context_stats.py evals/results/2026-11-12-deepseek-flash.json [--json out.json]
"""

import argparse
import json
import os
import sys
from pathlib import Path


def calls_by_run(log_dir: Path, run_ids: set[str]) -> dict[str, list[dict]]:
    out = {r: [] for r in run_ids}
    for f in sorted(log_dir.glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            rid = row.get("purpose", "").rsplit(":", 1)[-1]
            if rid in out and row.get("status") == "ok":
                out[rid].append(row)
    return {r: sorted(rows, key=lambda x: x["at"]) for r, rows in out.items()}


def stats(results: dict, log_dir: Path) -> dict:
    cases = [c for c in results["cases"] if c.get("run_id")]
    by_run = calls_by_run(log_dir, {c["run_id"] for c in cases})
    per_case = []
    for c in cases:
        rows = by_run[c["run_id"]]
        if not rows:
            continue
        prompts = [r["prompt_tokens"] for r in rows]
        per_case.append({"id": c["id"], "correct": c["correct"], "calls": len(rows), "total": sum(prompts),
                         "max": max(prompts), "by_step": prompts,
                         "cost_usd": round(sum(r.get("cost_usd") or 0 for r in rows), 6)})
    if not per_case:
        sys.exit("no logged calls matched this results file (a replay run?)")
    n = len(per_case)
    steps = max(len(p["by_step"]) for p in per_case)
    growth = []
    for i in range(steps):
        xs = [p["by_step"][i] for p in per_case if len(p["by_step"]) > i]
        growth.append({"step": i + 1, "cases": len(xs), "mean_prompt_tokens": round(sum(xs) / len(xs))})
    return {"results": results["summary"].get("run_at"), "model": results["summary"]["model"], "n": n,
            "mean_total_prompt_tokens": round(sum(p["total"] for p in per_case) / n),
            "max_total_prompt_tokens": max(p["total"] for p in per_case),
            "mean_max_prompt_tokens": round(sum(p["max"] for p in per_case) / n),
            "max_prompt_tokens": max(p["max"] for p in per_case),
            "mean_cost_usd": round(sum(p["cost_usd"] for p in per_case) / n, 6),
            "growth_by_step": growth, "cases": per_case}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results", type=Path)
    ap.add_argument("--log-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "data")) / "llm_calls")
    ap.add_argument("--json", type=Path, help="also write the full stats here")
    a = ap.parse_args(argv)
    s = stats(json.loads(a.results.read_text(encoding="utf-8")), a.log_dir)
    print(f"{s['model']}  n={s['n']}")
    print(f"prompt tokens per diagnosis: mean {s['mean_total_prompt_tokens']:,}  max {s['max_total_prompt_tokens']:,}")
    print(f"largest single call:         mean {s['mean_max_prompt_tokens']:,}  max {s['max_prompt_tokens']:,}")
    print(f"mean cost per diagnosis:     ${s['mean_cost_usd']:.4f}")
    print("mean prompt tokens by step:  " + "  ".join(f"{g['step']}:{g['mean_prompt_tokens']:,} (n={g['cases']})"
                                                     for g in s["growth_by_step"]))
    if a.json:
        a.json.write_text(json.dumps(s, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
