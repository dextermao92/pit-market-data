"""Compare eval runs at different context budgets against a benchmark run: accuracy, tokens, cache, cost.

Each argument is an eval results file (evals/results/*.json); the first is the benchmark. Token and cache figures
come from data/llm_calls (scripts/context_stats.py), cost at off-peak prices so runs at different hours compare.
Re-calls of a stubbed tool come from data/copilot_traces.

    uv run python scripts/budget_sweep.py evals/results/2026-11-12-deepseek-flash.json \
        evals/results/2026-11-13-deepseek-flash-budget-0.json ... [--json out.json]
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from context_stats import stats


def recalls_of_stubs(trace_dir: Path, run_ids: set[str]) -> dict[str, int]:
    """Per run: tool calls repeating (name, args) of a result that had been compacted earlier in that run."""
    stubbed, out = defaultdict(set), defaultdict(int)
    for f in sorted(trace_dir.glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            t = json.loads(line)
            rid = t.get("run_id")
            if rid not in run_ids:
                continue
            if t["kind"] == "compact":
                stubbed[rid] |= {(c["tool"], json.dumps(c["args"], sort_keys=True)) for c in t["compacted"]}
            elif t["kind"] == "tool" and (t["tool"], json.dumps(t.get("args"), sort_keys=True)) in stubbed[rid]:
                out[rid] += 1
    return out


def row(path: Path, log_dir: Path, trace_dir: Path) -> dict:
    res = json.loads(path.read_text(encoding="utf-8"))
    s, c = res["summary"], stats(res, log_dir)
    rids = {x["run_id"] for x in res["cases"] if x.get("run_id")}
    again = recalls_of_stubs(trace_dir, rids)
    budget = s.get("context_budget_tokens")
    return {"file": path.name, "run_at": s.get("run_at"), "budget": budget if budget else 0,
            "accuracy": s["accuracy_pct"], "real": s["by_kind"]["real"]["accuracy_pct"],
            "simulated": s["by_kind"]["simulated"]["accuracy_pct"], "correct": [x["id"] for x in res["cases"]
                                                                                if x["correct"]],
            "compacted": s.get("diagnoses_compacted", 0) or 0, "restubbed_calls": sum(again.values()),
            "calls": s["mean_llm_calls"], "tokens": c["mean_total_prompt_tokens"],
            "largest_call": c["max_prompt_tokens"], "mean_largest_call": c["mean_max_prompt_tokens"],
            "cache_hit": c["cache_hit_share"], "offpeak_cost": c["mean_offpeak_cost_usd"],
            "billed_cost": c["mean_cost_usd"], "peak_calls": c["peak_calls"]}


def pct(a, b):
    return f"{100 * (a - b) / b:+.0f} %" if b else "n/a"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results", nargs="+", type=Path, help="benchmark first")
    ap.add_argument("--log-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "data")) / "llm_calls")
    ap.add_argument("--trace-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "data")) / "copilot_traces")
    ap.add_argument("--json", type=Path)
    a = ap.parse_args(argv)
    rows = [row(p, a.log_dir, a.trace_dir) for p in a.results]
    bench = rows[0]
    print("| Run | Budget | Accuracy (real / sim) | Compacted | Re-called stubs | Calls | Prompt tokens / diag "
          "| Largest call | Cache hit | Cost / diag (off-peak) | vs benchmark |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(rows):
        budget = f"{r['budget']:,}" if r["budget"] else "none"
        print(f"| {'**benchmark** ' if i == 0 else ''}{r['run_at'][:10]} | {budget} "
              f"| {r['accuracy']} % ({r['real']} / {r['simulated']}) | {r['compacted']} / 25 | {r['restubbed_calls']} "
              f"| {r['calls']} | {r['tokens']:,} ({pct(r['tokens'], bench['tokens'])}) | {r['largest_call']:,} "
              f"| {r['cache_hit']:.1%} | ${r['offpeak_cost']:.4f} | {pct(r['offpeak_cost'], bench['offpeak_cost'])} |")
    if a.json:
        a.json.write_text(json.dumps(rows, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
